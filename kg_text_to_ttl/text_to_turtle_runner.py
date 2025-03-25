
from typing import List

import os
import sys
import os.path
import re
import traceback
import glob

import xml.etree.ElementTree as et
from rdflib import Graph

import tika
import fitz

from kg_text_to_ttl.text_to_turtle_processor import ProcessorException, UsageException
from kg_text_to_ttl.text_to_turtle_pdf_to_text import PdfTableRecognizer
# Make sure that Tika does not attempt to start a local server
tika.TikaClientOnly = True


class HelpException (UsageException):
    """Signals that the help text should be shown."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


def init_tika(server_addr:str):
    """Prepares the envionment and Tika library for connecting to a remote Tika server."""
    os.environ['TIKA_SERVER_ENDPOINT'] = server_addr
    os.environ['PYTHONIOENCODING']     = 'utf8'


def extract_document_text(filename:str, tika_proxy:str=None):
    """Extracts the text contents of the file with the given name by sending it to the Tika server.
    
    :param filename: The name or pathname of the file to parse.
    """
    from tika import parser
    proxies = None
    if tika_proxy == "" or tika_proxy == "-" or tika_proxy == "none":
        # Explictly disable proxies
        proxies = { "http": None, "https": None}
    elif tika_proxy is not None:
        # Explictly set a proxy
        proxies = { "http": tika_proxy, "https": tika_proxy}
    # Auto-disable proxies in case the Tika server is running on localhost and no proxies
    # are set explcitly
    endpoint = os.environ['TIKA_SERVER_ENDPOINT'] 
    if proxies is None and (endpoint.startswith("http://localhost:") or endpoint.startswith("https://localhost:")):
        proxies = { "http": None, "https": None}
    if proxies:
        parsed = parser.from_file(
            filename, 
            xmlContent=True,
            requestOptions= { 'proxies': proxies }
        )
    else:
        parsed = parser.from_file(
            filename, 
            xmlContent=True
        )
    return parsed


def extract_document_text_from_buffer(doc_buffer, doc_name:str, tika_server:str, tika_proxy:str=None):
    """Extracts the text contents of the (binary) document image by sending it to the Tika server.

    This function is for use in environments in which the document is not a file which can be read
    but instead is obatined otherwise (e.g. from a S3 bucket or via an API call) and stored in a
    buffer.
    
    :param doc_buffer: The document contents as a byte string
    :param name: The document base name
    :param tika_server: URL of the tika-endpoint of the Tika-server to use
    :param tika_proxy: if provided, the value is used for the http and https proxy settings
    :returns: The server response object
    """
    from os import environ
    from tika import parser
    from io import BytesIO
    proxies = None
    if tika_proxy == "" or tika_proxy == "-" or tika_proxy == "none":
        # Explictly disable proxies
        proxies = { "http": None, "https": None}
    elif tika_proxy is not None:
        # Explictly set a proxy
        proxies = { "http": tika_proxy, "https": tika_proxy}
    # Auto-disable proxies in case the Tika server is running on localhost and no proxies
    # are set explcitly
    # endpoint = environ['TIKA_SERVER_ENDPOINT'] 
    endpoint = tika_server
    if proxies is None and (endpoint.startswith("http://localhost:") or endpoint.startswith("https://localhost:")):
        proxies = { "http": None, "https": None}
    # Prepares the envionment and Tika library for connecting to a remote Tika server
    environ['TIKA_SERVER_ENDPOINT'] = tika_server
    environ['PYTHONIOENCODING']     = 'utf8'

    img_fh = BytesIO(doc_buffer)
    img_fh.name = doc_name
    if proxies:
        response = parser.from_file(
            img_fh, 
            xmlContent=True,
            requestOptions= { 'proxies': proxies }
        )
    else:
        response = parser.from_file(
            img_fh, 
            serverEndpoint=tika_server,
            xmlContent=True
        )
    return response


def get_document_content(filename:str, tika_proxy:str=None):
    response = extract_document_text(filename, tika_proxy=tika_proxy)
    content_string:str =  response['content']
    # The returned content string might conatin more than one root element.
    # So search for the end tag of the first root </html>, and ignore the
    # rest
    if content_string is None:
        return None
    page_end = content_string.find('</html>')
    first_page = content_string[0:page_end + 7]
    return first_page


def get_pdf_content(filename:str):
    """ Extracts XHTML text with table recognition and text line consolidation from a PDF.
    """
    doc = fitz.Document(filename)

    recognizer = PdfTableRecognizer()
    recognizer.process_doc(doc)

    img = recognizer.get_doc_image()

    return img


def glob_expand (name_or_pattern:str, target:list):
    if name_or_pattern.find("*") < 0 and name_or_pattern.find("?") < 0:
        # not a glob 
        target.append(name_or_pattern)
        return
    matches = glob.glob(name_or_pattern)
    if matches:
        target.extend(matches)
    else:
        target.append(name_or_pattern) # Append the pattern anyway so that an error is raised (file not found)



class PrintingLogWriter:

    def __init__(self, stream) -> None:
        self.stream = stream

    def write(self, string):
        # First, deletgate to the log stream
        self.stream.write(string)
        # Second, print it to the stdout
        print(string, end="")

    def close(self):
        self.stream.close()



class WorkflowResult:
    """Captures the result of a workflow execution."""

    def __init__(
        self,
        basename:str = None,
        graph:Graph = None,
        texts:dict = None,
        no_matches:int = None,
        no_triples:int = None,
        total_match_len:int = None,
        score:float = None
    ) -> None:
        self.basename = basename
        self.graph = graph
        self.texts = texts or {}
        self.no_matches = no_matches
        self.no_triples = no_triples
        self.total_match_len = total_match_len
        self.score = score


def run_text_to_turtle ():
    """ Runs one or more text to TTL workflows on one or more documents based on command line arguments.
    """
    from kg_text_to_ttl.text_to_turtle_processor import TextToTurtleProcessor, FileOutputHandler, StartdogGraphUploader, AzureStartdogGraphUploader
    tika_endpoint = os.environ.get("TIKA_ENDPOINT") or 'http://localhost:9998/tika'
    cmdname   = None
    docnames  = []
    workflows = []
    var_defs  = []
    tika_arg  = False
    verbose   = False
    very_vb   = False
    print_gp  = False
    out_dir   = None
    outp_arg  = False
    tgdb_arg  = False
    proxy_arg = False
    target_db = None
    doc_ns    = None
    verb      = None
    ns_arg    = False
    vdef_arg  = False
    proxy     = None
    for arg in sys.argv:
        if tika_arg:
            tika_endpoint = arg
            tika_arg = False
        elif outp_arg:
            out_dir = arg
            outp_arg = False
        elif tgdb_arg:
            target_db = arg
            tgdb_arg = False
        elif ns_arg:
            doc_ns = arg
            ns_arg = False
        elif proxy_arg:
            proxy = arg
            proxy_arg = False
        elif vdef_arg:
            var_defs.append(arg)
            vdef_arg = False
        elif arg == '-h':
            raise HelpException()
        elif arg == '-t':
            tika_arg = True
        elif arg == '-o':
            outp_arg = True
        elif arg == '-u':
            tgdb_arg = True
        elif arg == '-n':
            doc_ns = "."
        elif arg == '-N':
            ns_arg = True
        elif arg == '-put':
            verb = "PUT"
        elif arg == '-post':
            verb = "POST"
        elif arg == '-v':
            verbose = True
        elif arg == '-V':
            verbose = True
            very_vb = True
        elif arg == '-p':
            proxy_arg = True
        elif arg == '-d':
            vdef_arg = True
        elif arg == '-g':
            print_gp = True
        elif arg.startswith('-'):
            raise UsageException(f"invalid option: {arg}")
        elif cmdname is None:
            cmdname = arg
        elif arg.lower().endswith('.yaml'):
            glob_expand(arg, workflows)
            # workflows.append(arg)
        else:
            glob_expand(arg, docnames)
            # docnames.append(arg)
        # else:
        #    raise UsageException(f"Superfluous argument: {arg}")
    if not docnames:
        raise UsageException(f"Document argument missing")
    if not workflows:
        raise UsageException(f"Workflow argument missing")
    if tika_arg:
        raise UsageException(f"Tika endpoint option argument missing")
    if outp_arg:
        raise UsageException(f"Output option argument missing")
    if tgdb_arg:
        raise UsageException(f"Target DB option argument missing")
    if ns_arg:
        raise UsageException(f"Namespace option argument missing")
    if proxy_arg:
        raise UsageException(f"Proxy option argument missing")
    if vdef_arg:
        raise UsageException(f"Variable defintion argument missing")

    if out_dir is None:
        out_dir = os.getcwd()
    diag_dir = os.path.join(out_dir, 'diagnostics')

    if tika_endpoint is not None and  tika_endpoint.lower() in ["lh", "local", "localhost"]:
        tika_endpoint = "http://localhost:9998/tika"

    extra_vars = {}
    vard_re = re.compile(r"(.*?)=(.*)")
    for vdef in var_defs:
        m = vard_re.match(vdef)
        if m is None:
            raise UsageException(f"Invalid variable definition: \"{vdef}\"; must be <name>=<value>")
        vname = m.group(1)
        value = m.group(2)
        extra_vars[vname] = value

    init_tika(tika_endpoint)
    for docname in docnames:
        basename = os.path.basename(docname)
        trunkname, ext = os.path.splitext(basename)
        if verbose:
            print(f"Document: {docname}")
        clean_doc_name = re.sub("\s+", "-", trunkname)
        clean_doc_name = re.sub(r"[^\x20-\x7F]", "-", clean_doc_name)

        # Extract XHTML text from the document
        issue = None
        if ext.lower() == '.pdf':
            # PDF's are extracted via PyMuPDF and converted to XHTML internally
            if very_vb:
                print(f"Extracting text and tables form PDF")
            text = get_pdf_content(docname)
            if text is None:
                issue = f"WARNING: PyMuPDF no text for document {docname}; skipping"
        else:
            # Otherwise use Tika to extract XHTML/text out of documents
            if very_vb:
                print(f"Extracting text via Tika running at {tika_endpoint}")
            text = get_document_content(docname, tika_proxy=proxy)
            if text is None:
                issue = f"WARNING: Tika returned no text for document {docname}; skipping"
        if issue is not None:
            print(issue)
            log_stream = open(f"{eff_diag_dir}/{clean_doc_name}.log", 'w')
            print(issue, file=log_stream)
            continue

        results:List[WorkflowResult] = []
        for workflow in workflows:
            if verbose:
                print(f"Workflow: {workflow}")

            basename = os.path.basename(workflow)
            trunkname, _ = os.path.splitext(basename)
            clean_workflow_name = re.sub("\s+", "-", trunkname)

            eff_diag_dir = diag_dir
            if len(docnames) > 1:
                eff_diag_dir = os.path.join(eff_diag_dir, clean_doc_name)
            if len(workflows) > 1:
                eff_diag_dir = os.path.join(eff_diag_dir, clean_workflow_name)
            os.makedirs(eff_diag_dir, exist_ok=True)

            output_handler = FileOutputHandler(eff_diag_dir)

            log_stream = open(f"{eff_diag_dir}/{clean_doc_name}.log", 'w', encoding='utf-8')
            if very_vb:
                log_stream = PrintingLogWriter(stream=log_stream)
            processor = TextToTurtleProcessor(
                text, 
                output_handler=output_handler,
                log_stream=log_stream
            )
            processor.set_var('doc', clean_doc_name)
            processor.set_var('docname', os.path.basename(docname))
            processor.set_var('docpathname', docname)
            for n, v in extra_vars.items():
                processor.set_var(n, v)

            try:
                processor.execute_yaml_workflow(workflow)

                result = WorkflowResult(
                    basename = clean_doc_name,
                    graph = processor.graph(),
                    no_triples = processor.no_triplets(),
                    no_matches = processor.no_matches(),
                    total_match_len = processor.total_match_len(),
                    score = processor.score()
                )
                results.append(result)
                if verbose:
                    print(f"   #Triples: {result.no_triples:>6}")
                    print(f"   #Matches: {result.no_matches:>6}")
                    print(f"   MatchLen: {result.total_match_len:>6}")
            except ProcessorException as ex:
                print(f"ERROR: Executing workflow {workflow} on {docname} failed: {str(ex)}")
                f_name = f"{eff_diag_dir}/{clean_doc_name}.err"
                with open(f_name, 'w') as fh:
                    traceback.print_exception(ex, file=fh)
                print(f"       See {f_name} for details")
            finally:
                log_stream.close()

            if print_gp and len(processor.graph()) > 0:
                gt = processor.graph().serialize(format='turtle')
                print(gt)

        best:WorkflowResult = None
        if len(results) > 1:
            results.sort(key= lambda r: (r.no_triples, r.no_matches, r.total_match_len), reverse=True)
            best = results[0]
        elif len(results) == 1:
            best = results[0]
        if best:
            output_handler = FileOutputHandler(out_dir)
            output_handler.write_turtle(f"{best.basename}.ttl", best.graph)
            if target_db:
                # Also upload the graph to Stardog
                auth_method = os.environ.get('STARDOG_AUTH_METHOD')
                if auth_method in ['azure', 'azure-oauth2']:
                    token = login_via_msal()

                    uploader = AzureStartdogGraphUploader(
                        access_token=token,
                        client_id=None,
                        client_secret=None,
                        scope=None,
                        token_endpoint=None
                    )
                else:
                    uploader = StartdogGraphUploader(verbose=verbose)
                ns = doc_ns
                if doc_ns and doc_ns == ".":
                    ns = best.basename
                if verbose:
                    print(f"Uploading graph to {target_db}")
                uploader.upload(best.graph, target_db, verb=verb, graph_ns=ns)
        

def login_via_msal ():
    # See: https://learn.microsoft.com/en-us/entra/msal/python/
    from msal import PublicClientApplication

    app = PublicClientApplication(
        os.getenv('CLIENT_ID'),
        authority=os.getenv('AZURE_AUTHORITY'),
        # authority="https://login.microsoftonline.com/common"
    )

    result = None
    # Check if an account can be reused
    accounts = app.get_accounts()
    if accounts:
    # If so, you could then somehow display these accounts and let end user choose
        print("Pick the account you want to use to proceed:")
        for a in accounts:
            print(a["username"])
        # Assuming the end user chose this one
        chosen = accounts[0]
        # Now let's try to find a token in cache for this account
        # result = app.acquire_token_silent(["User.Read"], account=chosen)
        result = app.acquire_token_silent(scopes=[os.getenv('CLIENT_SCOPE')], account=chosen)
    
    if not result:
        print("NOTE: You are now authenticated at Stardog via Azure AD.")
        print("      In case you do NOT see the message 'Authenticated as <email>/<name>' below")
        print("      within a few seconds, please check for a newly opened browser tab or window")
        print("      and provide your credentials as requested.")
        result = app.acquire_token_interactive(scopes=[os.getenv('CLIENT_SCOPE')])

    if "access_token" in result:
        token = result["access_token"]
        name = None
        info = result.get('id_token_claims')
        if info is not None:
            name = info.get('preferred_username') or info.get('name')
        if name:
            print(f"Authenticated as {name}")
        else:
            print(f"Authenticated")
    else:
        raise ProcessorException ("Failed to obtain access token.")
    return token


def main_text_to_turtle():
    """To be called by the main-line of this script."""
    try:
        run_text_to_turtle()
    except UsageException as ex:
        show_detailed_help = False
        if isinstance(ex, HelpException):
            show_detailed_help = True
        else:
            print("USAGE ERROR")
            print(str(ex))
        print("""
USAGE

python -m kg_text_to_ttl [options]* <workflow>+ <doc>+ 

ARGUMENTS

  <workflow>    File containing a semantic property extraction workflow.
                All files having the extension .yaml are treated as workflow.
                At least one workflow must be provided.
                In case multiple workflows are provided, the graph of the one 
                containing the largest number of triplets is selected.
  <doc>         Document file. The document format must be recognized by
                Tika - the text extraction service. This includes
                *.docx, *.doc and *.pdf - among others.


OPTIONS

-t <tika_endpoint>
      The address of the Tika-server endpoint to use for converting
      documents (*.docx, *.doc, *.pdf, etc.) to text.
      Default is "lh" or "localhost" will be replaced with http://localhost:9998/tika

-o <output-dir>
      The output directory in which *.ttl and other files are
      created   

-d <var>=<value>
     Defines the variable with name <var> and sets it to <value>.
     This option may be repeated to set multiple variables.   

-u <stardog-db>
      Upload the result graph to a Stardog DB    
      <stardog-db>  URL of a Stardog DB to which the result graph is uploaded.
      This URL also contains the username and optionally also the password.
      The format is:

        http(s)://<username>(:<password>)@<host>/<db>

      if the password is not provided within the URL, it is tried to
      read it from the environment variable STARDOG_PASSWORD

-n    Upload to the graph (namespace) named like the file's basename without extension 
      (default mode is PUT)

-N <namespace>
      Upload to the graph to the given namespace
      (Default mode is PUT)

-put  Upload the graph via PUT. This overwrites the contents of the named graph.

-post Upload the graph via POST. This merges into the contents of the named graph.

-v    Verbose; print information to stdout

-V    Very verbose; print detailed information to stdout, including the log messages

-g    Print the resulting graph to stdout (if any)

-h    Help; Only show this help message. 
      Also describes how a local Tika server is run.

        """)
        if show_detailed_help:
            print("""
RUNNING TIKA
    Tika is used to convert popular document formats such as *.docx, *.doc or *.pdf
    to text/HTML documents which can be parsed by kg_text_to_ttl.
    If there's already a Tika server running in your network, use the -t option to
    specify its host, port and endpoint.

    In case no Tika server is available, you can easily run our own:

    1) Open a terminal (Linux) or Command Prompt (Windows, the cmd.exe).

    2) Make sure that Java is installed on your computer. Try

         java -version

       That should print a version message. If the executable java is not found,
       please install Java first .

    3) Download the Tika server(!) jar from https://tika.apache.org/download.html.
       Its name is something like "tika-server-standard-2.5.0.jar"
       Please make sure that the jar-file is put to the current directory
       (or move it from the Downloads-folder to it)

    4) Start the Tika server with

        java -jar <name-of-the-tika-jar-just-downloaded>

      e.g.:

        java -jar tika-server-standard-2.4.1.jar
       
      Now you should see some log messages appear in the terminal. 
      One of them should be similar to

       ... Started Apache Tika server 84fdd25b-d522-495e-9e23-90c124bbf2ff at http://localhost:9998/

      Now you can use kg_text_to_ttl on document formats such as *.doc(x). 
      It also not needed to use the -t option as the server is running locally.

    5) When done, hitting Cntrl-C in the terminal should stop the server  

        """)
    except Exception as ex:
        import traceback
        print("ERROR")
        print(str(ex))
        print("\nTRACEBACK")
        traceback.print_exception(ex)
        

if __name__ == '__main__':
    main_text_to_turtle()