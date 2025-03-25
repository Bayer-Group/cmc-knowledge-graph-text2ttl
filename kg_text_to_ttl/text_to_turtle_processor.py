
from enum import Flag
from typing import Dict, List, Any, Tuple, Union

import sys
import re
import os
import importlib
import urllib
import urllib.parse
import html
from datetime import date, datetime

import yaml
import requests
import requests.auth

from rdflib import ConjunctiveGraph, Namespace, Graph, XSD
from rdflib.term import URIRef, Literal

from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings
disable_warnings(InsecureRequestWarning)



class ProcessorException (Exception):
    """Signals issues when processing a text to extract semantic properties."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class UsageException (Exception):
    """Used to signal issues with using the Text to Turtle runner."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)




class OutputHandler:
    """Base class for output handlers.
    
    An ouput handler is repsonsible for storing the created graph
    in a turtle repesentation, e.g. as a file or an AWS S3 object, and
    for storing text blocks in a similar way.

    This class is meant to be sub-classed.
    """

    def __init__(self) -> None:
        pass

    def write_turtle(self, name:str, graph:Graph):
        """ Writes the given graph into a file with the provided (base-)name. """
        raise Exception(f"write_graph not implemented in {self.__class__.__name__}")

    def write_text(self, name:str, text:str):
        """ Writes the given text into a file with the provided (base-)name. """
        raise Exception(f"write_text not implemented in {self.__class__.__name__}")



class FileOutputHandler (OutputHandler):
    """Output handler that is writing files.
    
    :param basedir: The base output directory in which output files ar stored.
    """

    def __init__(self, basedir=".") -> None:
        super().__init__()
        self._basedir = basedir


    def write_turtle(self, name:str, graph:Graph):
        if not name.lower().endswith('.ttl'):
            name = f"{name}.ttl"
        gt = graph.serialize(format='longturtle')
        fn = os.path.join(self._basedir, name)
        with open(fn, 'w', encoding='utf8') as fh:
            fh.write(gt)


    def write_text(self, name:str, text:str):
        if not name.lower().endswith('.txt'):
            name = f"{name}.txt"
        fn = os.path.join(self._basedir, name)
        with open(fn, 'w', encoding='utf8') as fh:
            fh.write(text)


class QueryHandler:
    """ Handles SPARQL queries to graphs.
    
    This class is meant to be sub-classed.    
    """

    def query(
        self,
        selected_vars:List[str],
        bound_vars: Dict[str,Any], 
        from_graph:str,
        where_clause:str,
        cache:dict,
        username:str = None,
        password:str = None,
        handler = None
    ) -> Tuple[bool,List]:
        """ Executes a query on a graph an either returns the single resut or calls a handler for each result row.

        :param selected_vars: The variables that should be included in the result.
        :param bound_vars: A dictionary that contains values for each variable that should be bound to a value
        :param from_graph: The graph to query
        :where_clause: The query to execute
        :cache: A dictionary instance that is passed to each query call which can be used to cache things, e.g. a graph loaded from a file
        :param username: The user of a quer to a remote server
        :param password: The users password
        :param handler: Called for each row of the result (if pressent)
        :returns: A list of dictionaries mapping each selected variable to its result value
        """
        raise Exception(f"query not implemented in {self.__class__.__name__}")


class QueryDispatchHandler (QueryHandler):
    """Dispatches a query either to a local handler or a Stardog handler dependent on the graph argument."""


    def __init__(self, local_query_handler = None, stardog_query_handler = None) -> None:
        super().__init__()
        self._local_query_handler = local_query_handler or LocalQueryHandler()
        self._stardog_query_handler = stardog_query_handler or StardogQueryHandler()


    def query(
        self,
        selected_vars: List[str], 
        bound_vars: Dict[str,Any], 
        from_graph: str, 
        where_clause: str,
        cache:dict,
        username:str = None,
        password:str = None,
        handler = None
    ) -> Tuple[bool,List]:
        """Delegates the query either to the local or to the Stardog query handler.

        The query is delegated to the Stardog query handler if

        - the value of `from_graph` starts with "http:" or "https:"
        - the value of `from_graph` is prefixed with / starts with "<stardog>"

        Otherwise it is directed to the local query handler.
        """
        if from_graph.startswith("http:") or from_graph.startswith("https:") or from_graph.startswith("<stardog>"):
            return self._stardog_query_handler.query(
                selected_vars=selected_vars, bound_vars=bound_vars, from_graph=from_graph,
                where_clause=where_clause, cache=cache, username=username, password=password, handler=handler
            )
        return self._local_query_handler.query(
            selected_vars=selected_vars, bound_vars=bound_vars, from_graph=from_graph,
            where_clause=where_clause, cache=cache, username=username, password=password, handler=handler
        )
        


class LocalQueryHandler (QueryHandler):
    """ Handles queries that operate on the local graph that is just under construction. """

    def __init__(self) -> None:
        super().__init__()

    def query(
        self,
        selected_vars: List[str], 
        bound_vars: Dict[str,Any], 
        from_graph: str, 
        where_clause: str,
        cache:dict,
        username:str = None,
        password:str = None,
        handler = None
    ) -> Tuple[bool,List]:
        graph = self._load_graph(from_graph, cache)

        try:
            response = graph.query(where_clause)
        except Exception as ex:
            raise ProcessorException(f"Error in where clause '{where_clause}: {str(ex)}'") from ex

        result = []
        for row in response:
            values = {}
            for v in selected_vars:
                values[v] = row[v]
            result.append(values)
        return result


    def _load_graph(self, graph_id:str, cache:dict) -> Graph:
        """ Loads the graph with the given id and stores it in cache.
        
        In the case a graph with that id is already in cache, that is reused.
        """
        graph = cache.get(graph_id)
        if graph is None:
            graph = Graph()
            try:
                graph.parse(graph_id)
            except Exception as ex:
                raise ProcessorException(f"Failed to load graph '{graph_id}'") from ex
            cache[graph_id] = graph
        return graph


class StardogQueryHandler (QueryHandler):
    """ Supports querying any graphs hosted by the Stardog server.
    
    :param stardog_url_var: The environment variable from which take the value 
                            that replaces the `<stardog>` placeholder in a graph URL.
    :param proxies: A requests-library compatible proxies dictionary
    :param verify: Tells if to verify the SSL connection
    """

    def __init__(self, stardog_url_var:str = None, proxies:dict = None, verify:bool = False) -> None:
        super().__init__()
        self._server_url_var = stardog_url_var or "STARDOG_SERVER"
        self._proxies = proxies
        self._verify = verify


    def query(
        self,
        selected_vars: List[str], 
        bound_vars: Dict[str,Any], 
        from_graph: str, 
        where_clause: str,
        cache:dict,
        username:str = None,
        password:str = None,
        handler = None
    ) -> Tuple[bool,List]:
        # Resolve the <stardog> prefix if present
        if from_graph.startswith("<stardog>"):
            stardog_base_url = os.environ.get(self._server_url_var)
            if stardog_base_url is None:
                raise UsageException(f"Stardog server URL environment variable {self._server_url_var} is not defined or empty")
            from_graph = stardog_base_url + from_graph[9:]
        if from_graph is None or not isinstance(from_graph, str) or len(from_graph) < 5:
            raise UsageException(f"Invalid Stardog DB URL: {str(from_graph)}")
        
        # Build the API URL            
        postURL = from_graph
        if postURL[-1] != '/':
            postURL += '/'     
        encoded_where_clause = urllib.parse.quote(where_clause,safe='')
        # API method + query prefix
        method = f"query?query={encoded_where_clause}"
        # build of query part 1
        # build final url for API
        query = postURL + method
        # print(query)

        # Now call the Stardog server
        try:
            headers = {
                'Accept': 'application/sparql-results+json'
            }
            response = requests.post(
                query,
                headers=headers,
                auth = requests.auth.HTTPBasicAuth(username, password),
                proxies = self._proxies,
                verify = self._verify
            )
        except Exception as ex:
            raise ProcessorException(f"Calling the Stardog API failed: {str(ex)}") from ex

        if response.status_code != 200:
            raise ProcessorException(f"Stardog select request failed [{response.status_code}]: {str(response.text)}")
        
        # JSON Response structure:
        # {"head":{"vars":["s","p","o"]},"results":{"bindings":[]}}
        result = response.json()
        head   = result.get('head')
        results= result.get('results')
        if head is None or results is None:
            raise ProcessorException(f"Invalid Stardog select response; head or results entry missing: {str(result)}")
        vars     = head.get('vars')
        if vars is None:
            raise ProcessorException(f"Invalid Stardog select response; vars entry missing: {str(head)}")
        bindings = results.get('bindings')
        if bindings is None:
            raise ProcessorException(f"Invalid Stardog select response; bindings entry missing: {str(results)}")

        # Example results:
        # {
        #   "p": {"type":"uri","value":"http://www.w3.org/1999/02/22-rdf-syntax-ns#type"},
        #   "s": {"type":"uri","value":"https://github.com/Bayer-Group/cmc-ontologies/2000/0/bhcMaterialName"},
        #   "o": {"type":"uri","value":"http://www.w3.org/2002/07/owl#ObjectProperty"}
        # }
        # {
        #   "p": {"type":"uri","value":"https://github.com/Bayer-Group/cmc-ontologies/kos/19014/definition"},
        #   "s": {"type":"uri","value":"https://github.com/Bayer-Group/cmc-ontologies/2000/0/CPDDepartment"},
        #   "o": {"xml:lang":"en","type":"literal","value":"Departments in CMC. E.g., ChD FD etc."}
        # }
        res_list = []
        for bd in bindings:
            values = {}
            for var in vars:
                if var not in selected_vars:
                    continue
                value_spec  = bd.get(var)
                if value_spec is None or 'type' not in value_spec or 'value' not in value_spec:
                    print(f"WARNING: Invalid Stardog result binding for '{var}': {str(value_spec)}")
                    continue
                if value_spec['type'] == 'literal':
                    value = Literal(value_spec['value'], lang=value_spec.get('xml:lang'))
                else:
                    value = URIRef(value_spec['value'])
                values[var] = value
            if handler is None:
                res_list.append(values)
            else:
                handler(values)
        return res_list


class StartdogGraphUploader:
    """Uploads a graph to a Stardog DB. 
    
    This class assumes HTTP Basic auth to be used for authentication
    by Stardog when usin the API.
    """

    def __init__(
        self, 
        stardog_url_var:str = None, 
        password_var:str = None, 
        username:str = None,
        password:str = None,
        proxies:dict = None, 
        verify:bool = False, 
        verbose:bool = False,
        **kwargs # Other uploaders might support other keywords
    ) -> None:
        super().__init__()
        self._server_url_var = stardog_url_var or "STARDOG_SERVER"
        self._password_var = password_var or "STARDOG_PASSWORD"
        self._proxies = proxies
        self._verify = verify
        self._verbose = verbose
        self._username = username
        self._password = password


    def upload(
        self,
        graph:Graph,
        to_db: str, 
        graph_ns:str = None,
        verb:str     = None,
        username:str = None,
        password:str = None,
        ** kwargs # Ignored; other uploaders might support other asrguments
    ) :
        # Resolve the <stardog> prefix if present
        if to_db.startswith("<stardog>"):
            stardog_base_url = os.environ.get(self._server_url_var)
            if stardog_base_url is None:
                raise UsageException(f"Stardog server URL environment variable {self._server_url_var} is not defined or empty")
            to_db = stardog_base_url + to_db[9:]
        if to_db is None or not isinstance(to_db, str) or len(to_db) < 5:
            raise UsageException(f"Invalid Stardog DB URL: {str(to_db)}")

        user_info_pattern = re.compile(r'(https?)://([^:@]+)(:([^@]+))?@(.+)')
        m = user_info_pattern.match(to_db)
        if m:
            username = m.group(2)
            password = m.group(4)
            if password is not None and password.startswith('$'):
                pv = os.environ.get(password[1:])
                if pv is None:
                    raise UsageException(f"Password environment variable {password[1:]} not defined")
                password = pv
            to_db = f"{m.group(1)}://{m.group(5)}"
        if self._password is None and password is None:
            password = os.environ.get(self._password_var)

        if username:
            self._username = username
        if password:
            self._password = password

        if self._username is None:
            UsageException(f"No username provided for accessing Stardog DB")
        if self._password is None:
            UsageException(f"No password provided for accessing Stardog DB")
        
        # Build the API URL            
        post_url = to_db
        if graph_ns is not None:
            post_url += f"?graph=urn:doc:{graph_ns}"
            if verb is None:
                verb = "PUT"
        if verb is None:
            verb = "POST"
        if self._verbose:
            print(f"Uploading graph to {post_url}")

        gt = graph.serialize(format='turtle')

        # Now call the Stardog server
        try:
            headers = {
                'Content-Type': 'text/turtle'
            }
            response = requests.request(
                verb,
                post_url,
                data=gt.encode('utf-8'),
                headers=headers,
                auth = requests.auth.HTTPBasicAuth(self._username, self._password),
                proxies = self._proxies,
                verify = self._verify
            )
        except Exception as ex:
            raise ProcessorException(f"Calling the Stardog API failed: {str(ex)}") from ex

        if response.status_code not in [200, 201]:
            raise ProcessorException(f"Stardog upload request failed [{response.status_code}]: {str(response.text)}")
        
    

class AzureStartdogGraphUploader:
    """Uploads a graph to a Stardog DB. 
    
    This class assumes the Azure-flavour of Oauth2 is used for authentication
    by Stardog when using its API.
    """

    def __init__(self, 
        *, # Allow only keyword paramters         
        client_id:str, 
        client_secret:str,
        scope:str,
        token_endpoint:str,
        stardog_url_var:str = None, 
        password_var:str = None,
        proxies:dict = None, 
        access_token:str = None,
        verify:bool = False,
        verbose:bool = False,
        **kwargs
    ) -> None:
        super().__init__()
        self._server_url_var = stardog_url_var or "STARDOG_SERVER"
        self._password_var = password_var or "STARDOG_PASSWORD"
        
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._token_endpoint = token_endpoint 
        self._access_token = access_token
        self._max_retries = 3

        self._proxies = proxies
        self._verify = verify
        self._verbose = verbose


    def upload(
        self,
        graph:Graph,
        to_db: str, 
        graph_ns:str = None,
        verb:str     = None,
        client_id:str = None,
        client_secret:str = None,
        scope:str = None,
        **kwargs # Ignored, other uploader might support other options
    ) :
        if client_id:
            self._client_id = client_id
        if client_secret:
            self._client_secret = client_secret
        if scope:
            self._scope = scope

        # Resolve the <stardog> prefix if present
        if to_db.startswith("<stardog>"):
            stardog_base_url = os.environ.get(self._server_url_var)
            if stardog_base_url is None:
                raise UsageException(f"Stardog server URL environment variable {self._server_url_var} is not defined or empty")
            to_db = stardog_base_url + to_db[9:]
        if to_db is None or not isinstance(to_db, str) or len(to_db) < 5:
            raise UsageException(f"Invalid Stardog DB URL: {str(to_db)}")

        if self._access_token is None:
            # No access token provided when creating this uploader, so we need
            # the coordinates to create one.
            if self._client_id is None:
                raise UsageException(f"No client ID provided for accessing Stardog DB")
            if self._client_secret is None:
                raise UsageException(f"No client secret provided for accessing Stardog DB")
            if self._token_endpoint is None:
                raise UsageException(f"No token endpoint provided for accessing Stardog DB")
        
        # Build the API URL            
        post_url = to_db
        if graph_ns is not None:
            post_url += f"?graph=urn:doc:{graph_ns}"
            if verb is None:
                verb = "PUT"
        if verb is None:
            verb = "POST"
        if self._verbose:
            print(f"Uploading graph to {post_url}")

        gt = graph.serialize(format='turtle')

        tries = 0
        while tries < self._max_retries:
            tries += 1

            if self._access_token is None:
                self.obtain_access_token()

            # Now call the Stardog server
            try:
                headers = {
                    'Content-Type': 'text/turtle',
                    "Authorization": f"Bearer {self._access_token}"
                }
                response = requests.request(
                    verb,
                    post_url,
                    data=gt.encode('utf-8'),
                    headers=headers,
                    proxies = self._proxies,
                    verify = self._verify
                )
            except Exception as ex:
                raise ProcessorException(f"Calling the Stardog API failed: {str(ex)}") from ex

            if response.status_code in [401, 407]:
                # Authorization error. So reset the token an try again.
                self._access_token = None
                continue

            if response.status_code not in [200, 201]:
                raise ProcessorException(f"Stardog upload request failed [{response.status_code}]: {str(response.text)}")
        
            break # No more retries needed


    def obtain_access_token(self):
        """ Fetches the token used for authentication in the actual API call. """
        token_data = {
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "scope": self._scope,
            "grant_type": "client_credentials",
            "claims": "{\"roles\": [\"api-access\"]}"  # Replace with your wanted roles
        }

        token_response = requests.post(self._token_endpoint, data=token_data)

        if token_response.status_code == 200:
            self._access_token = token_response.json().get("access_token")
            if self._access_token is None:
                raise ProcessorException("Failed to get token from token response")
        else:
            raise ProcessorException(f"Token retrieval failed[{token_response.status_code}]: {token_response.text}")
        
        return self._access_token

    

class TextToTurtleProcessor:
    """ Extracts semantic properties in form of RDF Triples (exported as *.ttl/Turtle)
        from a text string orchestrated by an extraction workflow in YAML format.

        The extaction script is a nested set of lists of dictionaries in YAML format.
        For example, the YAML snipped::

            - match: foo (\d+)
              as: foo-number
              do:
                - echo: Foo: @{foo-number.1}

        means that in the current text it should look for the occurence of the text matching
        the regular expression (RE) 'foo (\d+)', so the string "foo " followed by one or more 
        digits, e.g. "foo 123". In case such a string is found, the match object is
        saved as 'foo-number'. In addition, all the operations in the
        do:-list are executed (otherwise they would be skipped).
        Here, the echo-operation is called. This would print the string "Foo: 123" to the log stream.
        The placeholder @{foo-number.1} in the echo value is replaced with the value of the first group of
        the match stored in foo-number.

        In general, individual operations of an extaction script have the form:

        - <keyword>: <keyword argument>
          <parameter-1>: <parameter-1 argument>
          <parameter-2>: <parameter-2 argument>
           ...
          <parameter-N>: <parameter-N argument>

        Keywords as well as parameters could also have complex values (lists or dictionaries).
        An example is the do: parameter of the match: operaion in the example above.

        :param text: The text from which to extract semantic properties.
    """

    def __init__(
        self,
        text:str="",
        log_stream=sys.stdout,
        output_handler:OutputHandler = None,
        query_handler:QueryHandler = None
    ) -> None:
        self._text = text
        self._slots = {}
        self._prefixes = {}
        self._triples = [] # Intermediate solution
        self._latest_match:re.Match = None
        self._matches:Dict[str,re.Match] = {}
        self._graph = ConjunctiveGraph()
        self._var:Dict[str,str] = {}
        self._mappings:Dict[str,Tuple[Dict,List[Tuple]],int] = {}
        self._input_graphs:Dict[str,Graph] = {}
        self._log_stream = log_stream
        self._log_task_level = 0
        self._imports = {}
        self._output_handler = output_handler or FileOutputHandler()
        self._query_handler = query_handler or QueryDispatchHandler()
        self._no_matches = 0
        self._total_match_len = 0
        self._no_triples = 0
        self._score = 0.0
        self._procedures = {} # Name to exec-steps mapping
        self._showTriples = False
        self._dim_tags_stack = []
        self._break_dimension = False



    def set_var(self, name:str, value):
        """ Sets a variable to the given value. """
        self._var[name] = value


    def text(self):
        """ Returns the current text."""
        return self._text


    def triplets(self):
        """ Returns the list of triples generated so far. """
        return self._triples


    def graph(self):
        """ Returns the graph build by the workflow. """
        return self._graph


    def no_matches(self) -> int:
        """ Returns the total number of matches made so far. """
        return self._no_matches


    def total_match_len(self) -> int:
        """ Returns the aggregated length of all the text that made up successful matches so far. """
        return self._total_match_len


    def no_triplets (self):
        """ Returns the number of triplets added to the graph so far. """
        return self._no_triples

        
    def score(self) -> float:
        """ Returns the scrore of the current graph. """
        return self._score


    def execute_yaml_workflow(self, filename:str):
        """ Executes a workflow stored in a YAML file. """
        with open(filename, 'r', encoding='utf8') as fs:
            plan = yaml.load(fs, yaml.Loader)
        self.execute_workflow(plan)


    def execute_yaml_workflow_stream(self, stream):
        """ Executes a workflow that is provided via a stream. """
        plan = yaml.load(stream, yaml.Loader)
        self.execute_workflow(plan)



    def execute_workflow(self, plan:list):
        """ Executes a Text to TLL workflow. 

        Executes the top-level operations one by one in order of appearance
        in the workflow.

        :param plan: The actual workflow as nested Python data stuctures.
        """
        for step in plan:
            if not isinstance(step, dict):
                raise ProcessorException(f"Plan step {str(step)} is not a dict")
            op = None
            for keyword, meth in TextToTurtleProcessor.keyword_2_method.items():
                if keyword in step:
                    op = meth
                    break
            if op is None:
                 raise ProcessorException(f"No operation key found in step: {str(step)}")      
            # Finally call the opetaion handler method
            op(self, step)
            

    #### Infrastructure / Helper Operations ####

    def nop(self, op:dict) -> Tuple[bool,Any]:
        """Operation that does nothing.
        
        May be used as placeholder operation.

        Invocation:
          pass: <ignored>

        Success: Always                                  
        """            
        return (True, None)


    def description(self, op:dict) -> Tuple[bool,Any]:
        """ Description string providing explict documentation. 
        
        The description string is not used in any particular way except that
        it is logged with a DESC marker.

        The string is also *not* subject to template expansion.

        One purpose of desc statements is to provide information that can be 
        securely parsed by simply handing the worklflow source to a YAML
        parser.

        Invocation:
          desc: <text>   Workflow desription string.

        Success: Always                                  
        """
        desc = self._get_step_attr(op, 'desc', str)
        self._log("DESC: ", desc)
        return (True, desc)


    def echo(self, op:dict) -> Tuple[bool,Any]:
        """ Echo the string to the console.
        
        The string argument is subject to template expansion, so echo can be used
        to print the current value of variables and match bindings

        Invocation:
          echo: <string>  Text to echo

        Success: Always                                  
        """
        message = self._get_step_attr(op, 'echo', str)
        message = self._expand_template(message)
        self._log("ECHO: ", message)
        # print(f">>{message}")
        return (True, message)


    def dump_text(self, op:dict) -> Tuple[bool,Any]:
        """ Writes the current text to a file.

        That is useful to get an image of a raw text file as delivered by Tika
        or the resut of text pre-processing steps.

        Invocation:
          dump: <what>   If "_", "*", or "ct", the current text is dumped.
                         Otherwise the string is exanded and dumped.
        Success: Always                                  
        """
        what:str  = self._get_step_attr(op, 'dump', str)
        fname:str = self._get_step_attr(op, 'file', str, optional=True)
        t = ""
        if what.strip() in ["_", "*", "ct", "current-text"]:
            t = self._text
        else:
            t = self._expand_template(what)
            
        if fname is not None:
            fname = self._expand_template(fname)
            self._output_handler.write_text(fname, t)
            self._info(f"Dumped current text to file {fname}")
        else:
            self._log("DUMP", t)
        return (True, fname)
        

    def use_any_of(self, op:dict) -> Tuple[bool,Any]:
        """ Executes its sub-operations until any of them is successful.
        
        Invocation:
          any-of: <sub-operations> Operations executed in order until one of them succeeds.        

        Success: if one of the sub-operation succeeds
        """
        sub_ops = self._get_step_attr(op, 'any-of',  list)
        return self._execute_seq(sub_ops, return_first_success=True)


    def assign_var(self, op:dict) -> Tuple[bool,Any]:
        """ Assigns a value to a variable. 
        
        The value can either be defined via to: or via expr:
        In the latter case the expanded value is used as argument to the Python eval function.

        Invocation:
          set: <varname>   The variable to set
          to: <value> the (new) value
          eval: <expr>  Python expression providing the (new) value

        Success: Always                                  
        """
        var   = self._get_step_attr(op, 'set', str)
        value = self._get_step_attr(op, 'to', optional=True)
        expr  = self._get_step_attr(op, 'eval', str, optional=True)
        imps  = self._get_step_attr(op, 'import', str, optional=True)

        if value is None and expr is None:
            raise ProcessorException(f"Neither to: or eval: attribute provided for setting variable {var}")
        if value is not None and expr is not None:
            raise ProcessorException(f"Both to: and eval: attributes provided for setting variable {var}")

        # Enables dynamic construction of variables. This allows for emulating
        # mappings and arrays
        var = self._expand_template(var)

        if expr:
            if imps is not None:
                for mod in re.split(r'\s+', imps):
                    if mod not in self._imports:
                        importlib.import_module(mod)
                        self._imports[mod] = True
                        self._info(f"Imported {mod}")
            expr = self._expand_template(expr)
            try:
                value = eval(expr, {'vars': self._var, 'matches': self._matches})
            except Exception as ex:
                raise ProcessorException(f"Evaluating expr for setting var {var} failed: {str(ex)}") from ex
        elif isinstance(value, str):
            value = self._expand_template(value)
        # print(f"{var} = {value}")
        self._var[var] = value
        return (True, value)
        

    def clear_var(self, op:dict) -> Tuple[bool,Any]:
        """ Clears a variable. 
        
        Resets the value of a variable to None (undefined).

        Invocation:
          clear: <varname> The variable to clear

        Success: Always                                  
        """
        var   = self._get_step_attr(op, 'clear', str)

        # Enables dynamic clearing of variables. This allows for emulating
        # mappings and arrays
        var = self._expand_template(var)

        self._var[var] = None

        return (True, None)
        

    def append_to_list(self, op:dict) -> Tuple[bool,Any]:
        """ Appends a value to a list.
        
        The value can either be defined via to: or via expr:
        In the latter case the expanded value is used as argument to the Python eval function.

        Invocation:
          append: <varname> The list variable to append to
          element: <element> The extra element

        Success: Always                                  
        """
        var   = self._get_step_attr(op, 'append', str)
        value = self._get_step_attr(op, 'element')

        # Enables dynamic construction of variables. This allows for emulating
        # mappings and arrays
        var = self._expand_template(var)

        list_val = self._var.get(var)
        if list_val is None or list_val == "":
            list_val = []
        elif not isinstance(list_val, list):
            list_val = [ list_val ]

        elem = self._expand_template(value)

        list_val.append(elem)

        self._var[var] = list_val

        return (True, value)
        


    def for_each_elem(self, op:dict) -> Tuple[bool,Any]:
        """ Executes a list of sub-operations for each element of a list. 
        
        Invocation:
          for-each: <list> Name of the list variable to traverse
          as: <varname> Variable in which the list elements are stored
          do: <list> List of sub-operations (loop body)

        Success: If the list contains at least one element
        """
        list_var  = self._get_step_attr(op, 'for-each')
        id        = self._get_step_attr(op, 'as',  str, optional=True)
        sub_steps = self._get_step_attr(op, 'do',  list)


        elems = self._var.get(list_var)
        if elems is None or elems == "":
            # raise ProcessorException(f"for-each list variable {list_var} not defined or empty")
            return (False, None)
        if  not isinstance(elems, list):
            raise ProcessorException(f"for-each list variable {list_var} is not a list")

        count = 1
        for elem in elems:
            if id is not None:
                self._var[id] = elem
                self._var[f"{id}_count"] = count 
            self._execute_seq(sub_steps)
            count += 1
 
        return (count > 1, None)


    def exec_python_code(self, op:dict) -> Tuple[bool,Any]:
        """ Executes Python code with access to variables and matches.

        Invocation:
          exec: <python-code>  Python code to execute.

        Success: Always                          
        """
        code   = self._get_step_attr(op, 'exec', str)
        # print(f"CODE: ***{code}***")
        exposed_as_globals = {
            'vars': self._var,
            'matches': self._matches
        }
        try:
            exec(code, exposed_as_globals)
        except Exception as ex:
            raise ProcessorException(f"Execution of Python code embedded in workflow via exec failed: {str(ex)}") from ex
        return (True, None)
        


    def save_as (self, op:dict) -> Tuple[bool,Any]:
        """ Stores the graph in a file as a Turtle (TTL) document. 
        
        Invocation:
          save-as: filename

        Success: Always                                          
        """
        target = self._get_step_attr(op, 'save-as')
        if isinstance(target, dict):
            f_name = self._get_step_attr(target, "file", str, optional=True)
            b_name = self._get_step_attr(target, "bucket", str, optional=True)
            if f_name is None and b_name is None:
                raise ProcessorException("Neither 'file' nor 'bucket' attribute specified for save-as")
            if f_name and b_name:
                # Check in which environment we operate and unset invalid option
                if self.is_aws_env():
                    f_name = None
                else:
                    b_name = None
        else:
            f_name = target
            b_name = None

        f_name = self._expand_template(f_name)
        self._output_handler.write_turtle(f_name, self._graph)
        self._info(f"Saved graph in {f_name}")
        return (True, f_name)


    def cond_exec(self, op:dict) -> Tuple[bool,Any]:
        """ Conditionally execute operations. 
        
        Invocation:
          if: <condition> A Python expression
          do: <list> Sub-operations to execute if the condition is true (if body)

        Success: if the condition is true, the success status of the body, else False
        """
        condition = self._get_step_attr(op, 'if')
        sub_steps = self._get_step_attr(op, 'do',  list)

        locals = self._var.copy()
        locals['matches'] = self._matches

        try:
            condition = self._expand_template(condition)
            result = eval(str(condition), {}, locals)
        except Exception as ex:
            raise ProcessorException(f"Invalid if-condition: {condition}") from ex

        if result: # Use Python truth
            return self._execute_seq(sub_steps)

        return (False, None)



    def _cond_def_exec(self, op:dict, op_name:str, negate:bool) -> Tuple[bool,Any]:
        """ Conditionally execute operations given that a variable or match is defined or not. """
        condition = self._get_step_attr(op, op_name)
        sub_steps = self._get_step_attr(op, 'do',  list)

        condition = self._expand_template(condition)

        defined = False
        m_m = re.match(r'(\w+)\.(\d+)', condition)
        if m_m:
            # Check for the defintion of a match group like "name.2"
            match_name  = m_m.group(1)
            match_group = int(m_m.group(2))
            m = self._matches.get(match_name)
            if m is not None:
                g = m.group(match_group)
                defined = (g is not None and g.strip() != "")
        else:
            # Check for the definition of a variable like "humidity"
            v = self._var.get(condition)
            defined = (v is not None and str(v).strip() != "")            

        if (defined and not negate) or (not defined and negate):
            return self._execute_seq(sub_steps)

        return (False, None)



    def ifdef_exec(self, op:dict) -> Tuple[bool,Any]:
        """ Conditionally execute operations given that a variable or match is defined. 

        If the name has the form <name>.<num>, it is checked if a match with this name exists,
        and if so, the match group with the given number is tested.
        Otherwise a variable with the given name is checked.
        The condition is true if the variable / match and group is defined and the value is
        not None and not the empty string.

        Invocation:
          idfdef: <variable or match> 
          do: <list> Sub-operations (ifdef-body)

        Success: If the condition is True, the result of the ifdef-body is used, otherwise it is False
        """
        return self._cond_def_exec(op, 'ifdef', False)


    def ifndef_exec(self, op:dict) -> Tuple[bool,Any]:
        """ Conditionally execute operations given that a variable or match is not defined. 

        If the name has the form <name>.<num>, it is checked if a match with this name exists,
        and if so, the match group with the given number is tested.
        Otherwise a variable with the given name is checked.
        The condition is true if the variable / match and group is not defined or the value is
        None or the empty string.

        Invocation:
          idfdef: <variable or match> 
          do: <list> Sub-operations (ifndef-body)

        Success: If the condition is True, the result of the ifndef-body is used, otherwise it is False
        """
        return self._cond_def_exec(op, 'ifndef', True)



    #### Procedures ####

    def def_procedure(self, op:dict) -> Tuple[bool,Any]:
        """ Defines a procedure. 
        
        A procedure is a sub-workflow that can be called with one or more arguments
        and that returns one ore more results.
        See also `call_procedure`.

        Procedures have an own namespace, so procedure names do not overalp with
        those of variables or matches.

        Invocation:
          procedure: <name> The name of the prcedure to define
          do: <list> Sub-operations (procedure body)

        Success: Always
        """
        name       = self._get_step_attr(op, 'procedure', str)
        proc_steps = self._get_step_attr(op, 'do',  list)

        if name in self._procedures:
            self._warning(f"Procedure {name} was already defined")
        self._procedures[name] = proc_steps
        return (True, None)


    def call_procedure(self, op:dict) -> Tuple[bool,Any]:
        """ Calls a procedure.
        
        Invokes a procedure with the arguments in the dictionary provided
        for with: This map binds the variable name provided as key to the
        given value only during the execution of the procedure.

        When execution of the procedure is finished, the values of the variables
        that appear as key are stored in the variables which names appear as value.
        
        Invocation:
          procedure: <name> The procedure to call
          with: <dict> argument key-value pairs
          return: <dict> Return value mapping from procedure variable (key) to caller variable (value)

        Success: If the last executed statement of the procedure body succeeded
        """
        name = self._get_step_attr(op, 'call')
        args = self._get_step_attr(op, 'with',  dict, optional=True)
        rtnv = self._get_step_attr(op, 'return',  dict, optional=True)

        proc_steps = self._procedures.get(name)
        if proc_steps is None:
            raise ProcessorException(f"Cannot find procedure {name}")
        
        try:
            # First save current var/match state
            vars = self._var
            mtch = self._matches
            lstm = self._latest_match

            # Make copies and update them with argument values
            self._var = self._var.copy()
            self._matches = self._matches.copy()
            if args is not None:
                for arg_name, arg_value in args.items():
                    if arg_value.startswith('~'):
                        match_name = arg_name[1:]
                        m = self._matches.get(match_name)
                        if m is None:
                            raise ProcessorException(f"Cannot find match named {match_name}")
                        self._matches[match_name] = m
                    else:
                        val = self._expand_template(arg_value)
                        self._var[arg_name] = val
            # Execute the procedure body
            s, v = self._execute_seq(proc_steps)

            if rtnv is not None:
                for arg_name, arg_target in rtnv.items():
                    v = self._var[arg_name]
                    vars[arg_target] = v

            return (s, v)
        finally:
            # Restore state
            self._var          = vars
            self._matches      = mtch
            self._latest_match = lstm


    #### Text Matching and Replacements ####

    def replace_pattern(self, op:dict) -> Tuple[bool,Any]:
        """ Replace text matching a pattern. 
        
        Note that the current text is modified (not the initial text).

        Invocation:
          replace: <pattern> The pattern that text to replace must match 
          with: <text> Replacement text

        Success: Always        
        """
        pattern = self._get_step_attr(op, 'replace', str)
        replacement = self._get_step_attr(op, 'with', str)
        reo = re.compile(pattern, flags=re.DOTALL)
        p_text = reo.sub(replacement, self._text)
        # self._info(f"Replaced '{pattern}' with '{replacement}'; Text size {len(self._text)} => {len(p_text)}")
        self._text = p_text
        return (True, None)


    def match_pattern(self, op:dict) -> Tuple[bool,Any]:
        """ Tries to match one of one or more patterns and if successful, executes sub-operations. 
        
        
        Invocation:
          match: <pattern|list of patterns> The pattern or a list of pattern 
          as: <matchname> Name of the match that keeps the result
          flags: <flags> Flags that control how text is matched
          do: <list> Sub-operations that are executed if a match was found (match-body)

        Success: If a match was found, the success status of the match-body, otherwise False
        """
        pattern   = self._get_step_attr(op, 'match')
        id        = self._get_step_attr(op, 'as',    str, optional=True)
        flags_lit = self._get_step_attr(op, 'flags', str, optional=True)
        sub_steps = self._get_step_attr(op, 'do',  list)

        flags     = self._resolve_flags(flags_lit, re.DOTALL)

        match:re.Match = None
        if isinstance(pattern, str):
            # Single RE
            reo:re.Pattern = re.compile(pattern, flags=flags)
            match = reo.search(self._text)
        elif isinstance(pattern, list):
            for p in pattern:
                if not isinstance(p, str):
                    raise ProcessorException(f"match list elemnt {str(p)} is not a string")
                reo:re.Pattern = re.compile(p, flags=flags)
                match = reo.search(self._text)
                if match is not None:
                    pattern = p
                    break
        else:
            raise ProcessorException(f"match value {str(pattern)} is neither a string (Single RE) nor a list (Multiple RE's)")

        if match is not None:
            self._latest_match = match
            if id is not None:
                self._matches[id] = match
            # self._info(f"Matched {pattern}")
            self._update_stats(match_inc= 1, match_len = len(match.group(0)))
            return self._execute_seq(sub_steps)
        ## This is too noisy in case a particular match is only one of several options
        ##
        # if isinstance(pattern, list):
        #     if len(pattern) > 3:
        #         pattern = pattern[0:3]
        #         pattern.append("...")
        #     self._warning(f"Failed to match any of  {', '.join(pattern)}")
        # else:
        #     self._warning(f"Failed to match {pattern}")
        return (False, None)


    def match_every(self, op:dict) -> Tuple[bool,Any]:
        """ Tries to match one of one or more patterns as often as possible and for each successful match, executes the sub-operations. 
        
        Invocation:
          match-every: <pattern> The pattern to match
          as: <matchname> Name of the match that keeps the result
          flags: <flags> Flags that control how text is matched
          do: <list> Sub-operations that are executed if a match was found (match-body)
          first: <list> If provuded, sub-operations that are executed for the first time (only) a match was found

        Success: True if at least one match was found
        """
        pattern   = self._get_step_attr(op, 'match-every')
        id        = self._get_step_attr(op, 'as',    str, optional=True)
        flags_lit = self._get_step_attr(op, 'flags', str, optional=True)
        sub_steps = self._get_step_attr(op, 'do',  list)
        do_first  = self._get_step_attr(op, 'first',  list, optional=True)

        flags = self._resolve_flags(flags_lit, re.DOTALL)

        match:re.Match = None
        reo:re.Pattern = re.compile(pattern, flags=flags)

        count = 1
        for match in reo.finditer(self._text):
            # if count == 1:
            #     self._info(f"Matched {pattern}")
            self._latest_match = match
            self._update_stats(match_inc=1, match_len=len(match.group(0)))
            if id is not None:
                self._matches[id] = match
                self._var[f"{id}_count"] = count 
            if count == 1 and do_first is not None:
                self._execute_seq(do_first)
            else:
                self._execute_seq(sub_steps)
            count += 1
        # if count == 1:
        #     self._warning(f"Failed to match {pattern}")
        # else:
        #     self._info(f"Matched {pattern} {count - 1} times")
        return (count > 1, None)


    def exec_within(self, op:dict) -> Tuple[bool,Any]:
        """Executes sub steps on the text of a group of a match.
        
        The sub-operations are only executed if the given pattern matches somewhere
        in the current text. While executing these steps, the current text is
        (temporarily) replaced with the group of the match.

        Invocation:
          within: <pattern> The pattern to match
          group: <group-index> Match group to use as current text (default: 1)
          as: <matchname> Name of the match that keeps the result
          flags: <flags> Flags that control how text is matched
          do: <list> Sub-operations that are executed if a match was found (match-body)

        Success: True if the pattern matched
        """
        pattern     = self._get_step_attr(op, 'within')
        group_index = self._get_step_attr(op, 'group', int, optional=True) or 1
        flags_lit   = self._get_step_attr(op, 'flags', str, optional=True)
        sub_steps   = self._get_step_attr(op, 'do',    list)
        id          = self._get_step_attr(op, 'as',    str, optional=True)

        flags     = self._resolve_flags(flags_lit, re.DOTALL)

        match:re.Match = None
        if isinstance(pattern, str):
            # Single RE
            reo:re.Pattern = re.compile(pattern, flags=flags)
            match = reo.search(self._text)
        elif isinstance(pattern, list):
            for p in pattern:
                if not isinstance(p, str):
                    raise ProcessorException(f"match list element {str(p)} is not a string")
                reo:re.Pattern = re.compile(p, flags=flags)
                match = reo.search(self._text)
                if match is not None:
                    pattern = p
                    break
        else:
            raise ProcessorException(f"match value {str(pattern)} is neither a string (Single RE) nor a list (Multiple RE's)")

        if match is not None:
            t = match.group(group_index)
            if t:
                # self._info(f"Matched within-scope {pattern}")
                self._update_stats(match_inc=1, match_len=len(match.group(0)))
                try:
                    if id is not None:
                        self._matches[id] = match
                    cur_text = self._text
                    self._text = t
                    self._execute_seq(sub_steps)
                finally:
                    self._text = cur_text
                return (True, t)
        # self._warning(f"Failed to match within-scope {pattern}")
        return (False, None)


    def exec_within_every(self, op:dict) -> Tuple[bool,Any]:
        """Executes sub steps on the text of a group of every match of a RE.
        
        The sub-operations executed for every match of the pattern somewhere
        in the current text. While executing these steps, the current text is
        (temporarily) replaced with the group of the match.

        Invocation:
          within-every: <pattern> The pattern to match
          group: <group-index> Match group to use as current text (default: 1)
          as: <matchname> Name of the match that keeps the result
          flags: <flags> Flags that control how text is matched
          do: <list> Sub-operations that are executed if a match was found
          first: <list> If provided, sub-operations that are executed for the first time (only) a match was found

        Success: True if the pattern matched
        """
        pattern     = self._get_step_attr(op, 'within-every')
        group_index = self._get_step_attr(op, 'group', int, optional=True) or 1
        flags_lit   = self._get_step_attr(op, 'flags', str, optional=True)
        sub_steps   = self._get_step_attr(op, 'do',    list)
        id          = self._get_step_attr(op, 'as',    str, optional=True)
        do_first    = self._get_step_attr(op, 'first',  list, optional=True)

        flags     = self._resolve_flags(flags_lit, re.DOTALL)

        match:re.Match = None
        reo:re.Pattern = re.compile(pattern, flags=flags)

        count = 1
        for match in reo.finditer(self._text):
            # if count == 1:
            #     self._info(f"Matched within-every scope {pattern}")
            self._latest_match = match
            self._update_stats(match_inc=1, match_len=len(match.group(0)))

            t = match.group(group_index)

            try:
                if id is not None:
                    self._matches[id] = match
                    self._var[f"{id}_count"] = count 
                cur_text = self._text
                self._text = t

                if count == 1 and do_first is not None:
                    self._execute_seq(do_first)
                else:
                    self._execute_seq(sub_steps)
                count += 1
            finally:
                self._text = cur_text

        return (count > 1, None)


    def exec_with(self, op:dict) -> Tuple[bool,Any]:
        """ Executes sub-operations on the text provided as an argument.
        
        The sub-operations are executed on the given text - which might be the
        result of template expnsion - as the current text. 

        Invocation:
          with: <value> Text to use as current text during execution of the sub-operations
          do: <list> Sub-operations that are executed with the provided value as current text

        Success: True if the given current text is not empty
        """
        text        = self._get_step_attr(op, 'with')
        sub_steps   = self._get_step_attr(op, 'do',    list)

        text = self._expand_template(text)

        if text is not None:
            try:
                cur_text = self._text
                self._text = text
                ok, result = self._execute_seq(sub_steps)
            finally:
                self._text = cur_text
            return (ok, result)

        return (False, None)




    def match_seq_of(self, op:dict) -> Tuple[bool,Any]:
        """Executes sub-steps for a sequence of alternatives or odered steps to match.

        This operation has two mutally exclusive modes: Looking for alternatives in order and looking 
        for matches in a strict step-by-step order. While doing any of this, the search start position is moved
        from the beginning of the current text towards the end.
        
        When the alternatives argument is defined, it is checked
        which of the given alternatives matches first, then the sub-operations for that altertive are executed
        and the search start is positioned behind the pattern match and then it is again looked for the nearest 
        matching alternative. This is done until the end of the current text is reached or no alternatives matches
        anymore.

        When the steps argument if defined, the pattern for the first step is matched in the current text.
        If it is found, the according sub-operations for this step are executed and the search start is positioned
        behind the pattern match. Then the same is tried for the second step. 
        This continues until a step does not match or the end of the current text is reached.
        
        While executing sub-operations of alternatives or steps, the current text is
        (temporarily) replaced with the group of the match.


        Invocation:
          sequence-of: <sequence-id> Id of this sequence 
          alternatives: List of alternatives for matches in the current text
            match: <pattern> The pattern of this alternative
            as: <matchname> Name of the match that keeps the result
            flags: <flags> Flags that control how text is matched
            do: <list> Sub-operations that are executed if a match was found (match-body)
            within-group: <index> If provided the match group that will be used as current text (defaut: 1)
          steps: <matchname> Name of the match that keeps the result
            match: <pattern> The pattern of this alternative
            as: <matchname> Name of the match that keeps the result
            flags: <flags> Flags that control how text is matched
            do: <list> Sub-operations that are executed if a match was found (match-body)
            within-group: <index> If provided the match group that will be used as current text (defaut: 1)

        Success: True if at least one alternative or step matched
        """
        seq_id       = self._get_step_attr(op, 'sequence-of')
        alternatives = self._get_step_attr(op, 'alternatives', list, optional=True)
        steps        = self._get_step_attr(op, 'steps',        list, optional=True)

        seq_id = seq_id or "<unnamed>"
        if alternatives is None and steps is None:
            raise ProcessorException(f"Neither 'alternatives' nor 'steps' argument provided for sequence-of: {seq_id}")
        if alternatives is not None and steps is not None:
            raise ProcessorException(f"Both 'alternatives' and 'steps' argument provided for sequence-of: {seq_id}; only one is allowed")
        
        specs = []
        counter = {}
        src = alternatives or steps
        for alt in src:
            pattern     = self._get_step_attr(alt, 'match')
            flags_lit   = self._get_step_attr(alt, 'flags', str, optional=True)
            sub_steps   = self._get_step_attr(alt, 'do',    list)
            id          = self._get_step_attr(alt, 'as',    str, optional=True)
            group       = self._get_step_attr(alt, 'within-group', int, optional=True)

            flags     = self._resolve_flags(flags_lit, re.DOTALL)

            if isinstance(pattern,list):
                reo:re.Pattern = [ re.compile(p, flags=flags) for p in pattern]
            else:
                reo:re.Pattern = re.compile(pattern, flags=flags)

            if id is not None:
                counter[id] = 0
            spec = {
                'pattern': pattern,
                'flags': flags,
                'reo': reo,
                'id': id,
                'group': group,
                'sub_steps': sub_steps
            }
            specs.append(spec)

        if alternatives is not None:
            count = 0
            start = 0
            matched = True
            while matched:
                matched = False

                earliest_start = None
                earliest_match = None
                earliest_spec  = None

                for spec in specs:
                    match:re.Match = None
                    if isinstance(spec['reo'], list):
                        match = self._try_res(spec['reo'], self._text, start)
                    else:
                        match = spec['reo'].search(self._text, start)
                    if match is not None:
                        m_start = match.start(0)
                        if earliest_start is None or m_start < earliest_start:
                            earliest_start = m_start
                            earliest_match = match
                            earliest_spec  = spec
                
                if earliest_match is not None:
                        self._latest_match = earliest_match
                        self._update_stats(match_inc=1, match_len=len(earliest_match.group(0)))
                        start = earliest_match.end(0)

                        if earliest_spec['id'] is not None:
                            self._matches[earliest_spec['id']] = earliest_match
                            counter_for_id = counter[earliest_spec['id']] + 1
                            counter[earliest_spec['id']] = counter_for_id
                            self._var[f"{earliest_spec['id']}_count"] = counter_for_id

                        count += 1
                        matched = True

                        if earliest_spec['group'] is not None:
                            t = earliest_match.group(earliest_spec['group'])
                            try:
                                cur_text = self._text
                                self._text = t

                                self._execute_seq(earliest_spec['sub_steps'])
                            finally:
                                self._text = cur_text
                        else:
                            self._execute_seq(earliest_spec['sub_steps'])

        else: # Strictly sequential
            count = 0
            start = 0
            for spec in specs:
                match:re.Match = None
                if isinstance(spec['reo'], list):
                    match = self._try_res(spec['reo'], self._text, start)
                else:
                    match = spec['reo'].search(self._text, start)
                if match is not None:
                    self._latest_match = match
                    self._update_stats(match_inc=1, match_len=len(match.group(0)))
                    start = match.end(0)

                    if spec['id'] is not None:
                        self._matches[spec['id']] = match

                    count += 1
                    matched = True

                    if spec['group'] is not None:
                        t = match.group(spec['group'])
                        try:
                            cur_text = self._text
                            self._text = t

                            self._execute_seq(spec['sub_steps'])
                        finally:
                            self._text = cur_text
                    else:
                        self._execute_seq(spec['sub_steps'])
                else:
                    break # Step did not match - so exit the sequence

        return (count >= 1, None)



    def two_dimenisonal_match(self, op:dict) -> Tuple[bool,Any]:
        """ Matches strcutures that have two dimesnions such a rows and columns of a table.
            DEPRECATED!
        """
        pattern_1   = self._get_step_attr(op, 'match-1')
        pattern_2   = self._get_step_attr(op, 'match-2')
        group_1     = self._get_step_attr(op, 'group-1',    int, optional=True) or 1
        group_2     = self._get_step_attr(op, 'group-2',    int, optional=True) or 1
        id_1        = self._get_step_attr(op, 'as-1',       str, optional=True)
        id_2        = self._get_step_attr(op, 'as-2',       str, optional=True)
        id_header   = self._get_step_attr(op, 'header-as',  str, optional=True)
        id_leading  = self._get_step_attr(op, 'leading-as', str, optional=True)
        flags_lit   = self._get_step_attr(op, 'flags',      str, optional=True)
        sub_steps   = self._get_step_attr(op, 'do',         list)
        do_first    = self._get_step_attr(op, 'first',      list, optional=True)
        do_leading  = self._get_step_attr(op, 'leading',    list, optional=True)

        flags = self._resolve_flags(flags_lit, re.DOTALL)

        match_1:re.Match = None
        match_2:re.Match = None
        reo_1:re.Pattern = re.compile(pattern_1, flags=flags)
        reo_2:re.Pattern = re.compile(pattern_2, flags=flags)

        header_matches = []

        # self._info(f"2DIM CT {self._text}")
        count_1 = 1
        for match_1 in reo_1.finditer(self._text):
            # if count == 1:
            #     self._info(f"Matched {pattern}")

            t = match_1.group(group_1)

            try:
                self._latest_match = match_1
                # self._update_stats(match_inc=1, match_len=len(match_1.group(0)))
                if id_1 is not None:
                    self._matches[id_1] = match_1
                    self._var[f"{id_1}_count"] = count_1

                cur_text = self._text
                self._text = t

                # Axis 2
                count_2 = 1
                for match_2 in reo_2.finditer(self._text):
                    # if count == 1:
                    #     self._info(f"Matched {pattern}")

                    #t2 = match_2.group(group_2)

                    self._latest_match = match_2
                    # self._update_stats(match_inc=1, match_len=len(match_1.group(0)))
                    if id_2 is not None:
                        self._matches[id_2] = match_2
                        self._var[f"{id_2}_count"] = count_2
                    if count_1 == 1:
                        # At the header row - so keep it as header match for re-use in every following row
                        header_matches.append(match_2)
                        # self._info(f"ADDED HEADER MATCH: {match_2.group(1)}")
                    if id_header is not None:
                        self._matches[id_header] = header_matches[count_2 - 1]
                        self._var[f"{id_header}_count"] = count_2
                    if count_2 == 1 and id_leading is not None:
                        self._matches[id_leading] = match_2
                        self._var[f"{id_leading}_count"] = count_1


                    if count_1 == 1 and do_first is not None:
                        # Execute the seq for every 
                        self._execute_seq(do_first)
                    elif count_2 == 1 and do_leading is not None:
                        # Execute the seq for every 
                        self._execute_seq(do_leading)
                    else:
                        # Execute the default seq for every cell
                        self._execute_seq(sub_steps)

                    count_2 += 1

                count_1 += 1
            finally:
                self._text = cur_text

        return (count_1 > 1, None)



    def multi_dimenisonal_match(self, op:dict) -> Tuple[bool,Any]:
        """ Matches structures that have multiple dimensions such a rows and columns of a table.

        Invocation:
          match-dimensions: <list-of-re> Regular expressions to match at each dimension
          dimensions: <name-list> name of each dimension. Must have the same length as the RE-list for match-dimensions
          as: <varname> Variable in which to store each match
          flags: <flags> RE macthing flags to be used
          do(-<dim>)+: <list> Sub-operations that are executed for every match of the final dimension
          pre: <sub-operations> Sub-operations to execute before handling the do-* blocks.

        Success: True if the final dimension RE has been matched at least once.
        """
        patterns    = self._get_step_attr(op, 'match-dimensions', list)
        cell_id     = self._get_step_attr(op, 'as',         str,  optional=True)
        dimensions  = self._get_step_attr(op, 'dimensions', list, optional=True)
        flags_lit   = self._get_step_attr(op, 'flags',      str,  optional=True)
        sub_steps   = self._get_step_attr(op, 'do',         list)
        pre_steps   = self._get_step_attr(op, 'pre',        list, optional=True)
        loc_steps   = self._get_matching_attrs(op, r'do-(\*|[0-9A-Za-z_]+)-(\*|[0-9A-Za-z_]+)')
        # loc_steps   = self._get_matching_attrs(op, r'do-(\*|\d+)-(\*|\d+)')

        flags = self._resolve_flags(flags_lit, re.DOTALL)

        reos = []
        for pattern in patterns:
            reo:re.Pattern = re.compile(pattern, flags=flags)
            reos.append(reo)

        if dimensions is not None and len(dimensions) != len(reos):
            raise ProcessorException(f"The dimensions name list does not have the same length as match-dimensions")

        counters = [None] * len(patterns)

        prev_matches = self._no_matches

        if pre_steps is not None:
            self._dim_tags_stack.append([None] * len(dimensions))
            try:
                dim_matches = self._prescan_dimension(0, reos=reos, counts=counters, dimensions=dimensions, cell_id=cell_id)

                self._execute_seq(pre_steps)

                self._walk_dimension(0, dim_matches=dim_matches, counts=counters, dimensions=dimensions, cell_id=cell_id, sub_steps=sub_steps, loc_steps=loc_steps)
            finally:
                self._dim_tags_stack.pop()
        else:
            # Now recurse into the match dimensions
            self._match_dimension(0, reos=reos, counts=counters, dimensions=dimensions, cell_id=cell_id, sub_steps=sub_steps, loc_steps=loc_steps)

        return (self._no_matches > prev_matches, None)


    def _prescan_dimension(self, dim:int, reos, counts, dimensions, cell_id) -> List:
        """ Used to scan the current text with the given RE's and assemble all matches in nested match rsult arrays. """
        dim_matches = []
        count = 1
        for match in reos[dim].finditer(self._text):
            counts[dim] = count
            
            if dimensions:
                # Counter value of each dimension should be saved
                self._var[dimensions[dim]] = count

            if dim < len(reos) - 1:
                t = match.group(1)
                try:
                    cur_text = self._text
                    self._text = t
                    # Recurse to next dimension
                    sub_dim_matches = self._prescan_dimension(dim + 1, reos, counts, dimensions, cell_id)
                    dim_matches.append(sub_dim_matches)
                finally:
                    self._text = cur_text
            else:
                # Final dimension reached

                # 1) Save the cell match
                if cell_id:
                    postfix = "-".join([str(i) for i in counts])
                    cell_key = f"{cell_id}-{postfix}"
                    self._matches[cell_key] = match
                    # self._matches[cell_id]  = match
                dim_matches.append(match)

            count += 1

        return dim_matches


    def _walk_dimension(self, dim:int, dim_matches, counts, dimensions, cell_id, sub_steps, loc_steps):
        """ Walks along the matches found by the _prescan_dimension method."""
        count = 1
        for match in dim_matches:
            counts[dim] = count
            
            if dimensions:
                # Counter value of each dimension should be saved
                self._var[dimensions[dim]] = count

            if dim < len(counts) - 1:
                self._walk_dimension(dim + 1, match, counts, dimensions, cell_id, sub_steps, loc_steps)
                self._break_dimension = False
            else:
                # Final dimension reached

                # Check if this run of the final dimension was aborted with break
                if self._break_dimension:
                    break

                self._update_stats(match_inc=1, match_len=len(match.group(0)))

                # 1) Save the cell match
                if cell_id:
                    postfix = "-".join([str(i) for i in counts])
                    cell_key = f"{cell_id}-{postfix}"
                    self._matches[cell_key] = match
                    self._matches[cell_id]  = match


                # 2) Find the most specific operation sequence
                index_seq = []
                ops = self._op_match(0, counts, index_seq, loc_steps)
                if ops is None:
                    ops = sub_steps

                # 3) Execute the matching op-seq                
                # self._execute_seq(ops)
                t = match.group(1)
                try:
                    cur_text = self._text
                    self._text = t
                    self._execute_seq(ops)
                finally:
                    self._text = cur_text

            count += 1



    def _match_dimension(self, dim:int, reos, counts, dimensions, cell_id, sub_steps, loc_steps):
        """ Incrementally matches the given RE's alomg the provided dimension. """
        count = 1
        for match in reos[dim].finditer(self._text):
            counts[dim] = count
            
            if dimensions:
                # Counter value of each dimension should be saved
                self._var[dimensions[dim]] = count

            if dim < len(reos) - 1:
                t = match.group(1)
                try:
                    cur_text = self._text
                    self._text = t
                    # Recurse to next dimension
                    self._match_dimension(dim + 1, reos, counts, dimensions, cell_id, sub_steps, loc_steps)
                finally:
                    self._text = cur_text
            else:
                # Final dimension reached

                self._update_stats(match_inc=1, match_len=len(match.group(0)))

                # 1) Save the cell match
                if cell_id:
                    postfix = "-".join([str(i) for i in counts])
                    cell_key = f"{cell_id}-{postfix}"
                    self._matches[cell_key] = match
                    self._matches[cell_id]  = match


                # 2) Find the most specific operation sequence
                index_seq = []
                ops = self._op_match(0, counts, index_seq, loc_steps)
                if ops is None:
                    ops = sub_steps

                # 3) Execute the matching op-seq                
                # self._execute_seq(ops)
                t = match.group(1)
                try:
                    cur_text = self._text
                    self._text = t
                    self._execute_seq(ops)
                finally:
                    self._text = cur_text

            count += 1



    def _op_match (self, dim:int, counts:list, indexes:list, loc_steps:dict):
        """ Looks for operation sequences that were defined in a do-* statement that matches the current indexes along all dimensions. """
        if dim == len(counts):
            postfix = "-".join([str(i) for i in indexes])
            key = f"do-{postfix}"
            ops = loc_steps.get(key)
            if ops is not None:
                # Found ops for this index sequence
                return ops
        else:
            if len(self._dim_tags_stack) > 0:
                dim_tags = self._dim_tags_stack[-1]
                tags = dim_tags[dim]
                if tags is not None:
                    tag = tags.get(counts[dim])
                    if tag is not None:
                        indexes.append(tag)
                        ops = self._op_match(dim + 1, counts, indexes, loc_steps)
                        if ops:
                            return ops
                        indexes.pop()
            indexes.append(counts[dim])
            ops = self._op_match(dim + 1, counts, indexes, loc_steps)
            if ops:
                return ops
            indexes[-1] = '*'
            ops = self._op_match(dim + 1, counts, indexes, loc_steps)
            if ops:
                return ops
            indexes.pop()
        return None


    def tag_dimension(self, op:dict) -> Tuple[bool,Any]:
        """ Tags a dimension at a specific index with a tag-name:

        Invocation:
          with: <value> text to use as current text during execution of the sub-operations
          do: <list> Sub-operations that are executed with the provided value as current text

        Success: Always
        """
        dim_no    = self._get_step_attr(op, 'tag-dimension')
        at_index  = self._get_step_attr(op, 'at',          )
        tag_name  = self._get_step_attr(op, 'as',          str)

        dim_no   = int(self._expand_template(str(dim_no)))
        at_index = int(self._expand_template(str(at_index)))

        if len(self._dim_tags_stack) < 1:
            raise ProcessorException(f"Use of tag-dimension not within dynamic scope of match-dimensions")
        
        dim_tags = self._dim_tags_stack[-1]
        if dim_no < 1 or dim_no > len(dim_tags):
            raise ProcessorException(f"Invalid tag-dimension: {dim_no}  (must be within [1,{len(dim_tags)}])")
        if at_index < 1:
            raise ProcessorException(f"Invalid tag-dimension at: index: {at_index}  (must be >= 1)")
        
        tags = dim_tags[dim_no - 1]
        if tags is None:
            tags = {}
            dim_tags[dim_no - 1] = tags
        tags[at_index] = tag_name

        return (True, None)


    def break_dimension(self, op:dict) -> Tuple[bool,Any]:
        """ Breaks out of the processing of the current match dimension. 

        This is similar to the break statmenets for loops in programming languages such as Python or Java.
        One use case is to stop processing the cells of a table row if a certain cell in a row does not
        fulfill a particular criteria.

        Invocation:
          braek: <ignored> 

        Success: Always
        """
        breaker = self._get_step_attr(op, 'break', optional=True)

        self._break_dimension = True

        return (True, None)



    #### Data Transformation Operations ####

    def def_mapping(self, op:dict) -> Tuple[bool,Any]:
        """ Defines a mapping from text to text and/or regular expressions to text. 
                
        Invocation:
          mapping: <name>
          flags: <flags>
          pairs: <list>
            - from: text to map from
            - re: regular expressions to map from
              to: text to map to

        Success: Always        
        """
        name = self._get_step_attr(op, 'mapping', str)
        pairs = self._get_step_attr(op, 'pairs', list)
        flags_lit = self._get_step_attr(op, 'flags', str, optional=True)

        flags = self._resolve_flags(flags_lit)
        ignore_case = (flags & re.IGNORECASE) != 0

        p_dict = {}
        p_re   = []
        for p in pairs:
            if not isinstance(p, dict):
                raise ProcessorException(f"Mapping element '{str(p)}' is not a dict")
            f_text = p.get('from')
            f_re   = p.get('re')
            f_to   = p.get('to')
            if f_to is None:
                raise ProcessorException(f"Mapping element '{str(p)}' has no 'to' attribute")
            if f_text:
                if f_re:
                    raise ProcessorException(f"Mapping element '{str(p)}' has both a 'from' and a 're' attribute")
                p_dict[f_text] = f_to
                if ignore_case:
                    p_dict[f_text.lower()] = f_to
            elif f_re:
                p_re.append( (f_re, f_to) )
            else:
                raise ProcessorException(f"Mapping element '{str(p)}' has neither a 'from' nor a 're' attribute")
        self._mappings[name] = (p_dict, p_re, flags)
        self._info(f"Defined mapping '{name}'; {len(p_dict)} simple entries, {len(p_re)} RE entries")
        # Mappig definitions have no explict result, therefore the name is returned
        return (True, name)


    def map_value(self, op:dict) -> Tuple[bool,Any]:
        """ Maps a given value to a target text as defined in the referred mapping. 
        
        Invocation:
          map: <value> The value to map
          apply: <name> The name of the mapping to use
          to: <varname> Name of the variable in which to store the mapping result

        Success: Always                
        """
        value   = self._get_step_attr(op, "map", str)
        mapping = self._get_step_attr(op, "apply", str)
        var     = self._get_step_attr(op, 'to', optional=True)
        
        # First,exand the value as many times the actual value
        # comes from a match or variable
        value = self._expand_template(value)

        # And now try to apply the referred mapping
        mapped = self._apply_mapping(mapping, value)

        if var is not None:
            # Store the mapped value in a variable if requested
            self._var[var] = mapped

        # And return the result
        return (True, mapped)


    def _apply_mapping(self, name:str, value:str) -> str:
        """ Apply the named value mapping to the given value.
        
        First it is tried to find a direct text match. If one is found
        its replacement is subject to template exansion and then returned.

        Then it is tried to match any of the RE's defined for the mapping 
        in the order of definition. If a macth is found, the replacement 
        string is subject to template expansion before it is returned.

        Otherwise the value is returned unchanged.
        """
        mapping = self._mappings.get(name)
        if mapping is None:
            raise ProcessorException(f"Unknown mapping: {name}")
        m_dict, m_re, flags = mapping
        
        repl = m_dict.get(value)
        if repl is not None:
            return self._expand_template(str(repl))
        
        if (flags & re.IGNORECASE) != 0:
            repl = m_dict.get(value.lower())
            if repl is not None:
                return self._expand_template(repl)

        saved_lm = self._latest_match
        try:
            for rt in m_re:
                pat, repl = rt
                m =  re.match(pat, value)
                if m:
                    self._latest_match = m
                    repl = self._expand_template(repl)
                    return repl
        finally:
            self._latest_match = saved_lm
        return value


    def query_graph (self, op:dict) -> Tuple[bool,Any]:
        """ Queries a graph and processes the results. 

        To avoid hadcoding usrenames and in particular password in a workflow, it is strongly
        recommended to stored the actual values in environment variables and use the name of
        these variables prefixed with a "$".
        
        Invocation:
          select: <query> String with query variables separated by one or more blanks
          from: <graph> Name of the graph that is queried
          where: <query> The SPARQL query to execute
          as: <varnames> String with result variables separated by one or more blanks
          username: <username> In case the username starts with a $, the actual username is read form the environment variable with that name
          password: <password> In case the password starts with a $, the actual password is read form the environment variable with that name
          do: Sub-operations to execute for each query result
          else: Sub-operations to execute if the query does not select anything

        Success: True if the query selected one or more results.
        """
        selection    = self._get_step_attr(op, 'select', str)
        graph_id     = self._get_step_attr(op, 'from', str)
        where_clause = self._get_step_attr(op, 'where', str)
        var_ids      = self._get_step_attr(op, 'as', str, optional=True) or selection
        username     = self._get_step_attr(op, 'username', str, optional=True)
        password     = self._get_step_attr(op, 'password', str, optional=True)
        else_clause  = self._get_step_attr(op, 'else', str, optional=True)
        row_steps    = self._get_step_attr(op, 'do', list, optional=True)
        
        # graph = self._load_graph(graph_id)

        # The where clause 
        select_vars  = re.split(r'\s+', selection)
        stored_vars  = re.split(r'\s+', var_ids)
        where_clause = self._expand_template(where_clause)

        if password is not None and password.startswith('$'):
            env_value = os.environ.get(password[1:])
            if env_value is None:
                raise ProcessorException(f"Password environment variable {password[1:]} is not defined")
            password = env_value
        if username is not None and username.startswith('$'):
            env_value = os.environ.get(username[1:])
            if env_value is None:
                raise ProcessorException(f"Username environment variable {username[1:]} is not defined")
            username = env_value
        
        if row_steps is None:
            result = self._query_handler.query(
                selected_vars = select_vars,
                bound_vars    = {}, 
                from_graph    = graph_id, 
                where_clause  = where_clause,
                cache         = self._input_graphs,
                username      = username,
                password      = password
            )
            no_rows = len(result)

            if no_rows == 0:
                if else_clause:
                    return self._execute_seq(else_clause)
                return (False, None)

            if no_rows > 1:
                self._warning(f"Multiple result rows in query of {graph_id} without do: argument")

            # for var_id, value in zip(stored_vars, result):
            #     self._var[var_id] = value
            for var, alias in zip(select_vars, stored_vars):
                self._var[alias] = result[0].get(var)
            # Return the first selected variable a operation result as the query might be an element of a first-successul-sequence
            return (True, result[0].get(select_vars[0]))
        else:
            no_rows = 0
            def row_handler(row_results:dict):
                for var, alias in zip(select_vars, stored_vars):
                    self._var[alias] = row_results.get(var)
                no_rows += 1
                self._execute_seq(row_steps)
                    
            result = self._query_handler.query(
                selected_vars = select_vars,
                bound_vars    = {}, 
                from_graph    = graph_id, 
                where_clause  = where_clause,
                cache         = self._input_graphs,
                username      = username,
                password      = password,
                handler       = row_handler
            )
            if no_rows == 0:
                if else_clause:
                    return self._execute_seq(else_clause)
                return (False, None)

        return (True, None)
        
            

    #### RDF Graph Output Related Operations ####

    def def_prefix(self, op:dict) -> Tuple[bool,Any]:
        """Defines a prefix for an IRI.
        
        Format:
          prefix: <prefix> The prefix name 
          iri: <iri> The IRI for which the prefix is used

        Success: Always
        """
        prefix = self._get_step_attr(op, 'prefix', str)
        iri = self._get_step_attr(op, 'iri', str)
        ns = Namespace(iri)
        self._prefixes[prefix] = ns
        self._graph.bind(prefix, ns)
        return (True, prefix)



    def def_triple(self, op:dict, implied_subject=None, implied_predicate=None, implied_inverse=None):
        """ Adds one or more triples to the result graph. 
        
        Invocation:
          subject: <subject-iri>
          predicate: <predicate-iri>
          inverse: <predicate-iri>
          object: <object>
          pedicates:
            - predicate: <predicate-iri>
              inverse: <predicate-iri>
              object: <object>
              objects:
                - object: <object>
          objects:
            - object: <object>

        Object defintion can either have the simple form "object: <object>", but it n also have
        additional arguments provided as a dictionary:

        object:
          text: <text-value>
          lang: The language code for text literals, e.d. "en" or "de"
          iri: <iri-literal> This can be either a complete URL or a named prefix of the form some-prefix:some-name
          integer: <integer-literal>
          float: <float-literal>
          bool: <boolean-literal>
          apply: <mappig-name> mapping to apply before creatong the object value

        Success: Always        
        """
        subject   = self._get_step_attr(op, 'subject',   str, optional=True)
        predicate = self._get_step_attr(op, 'predicate', str, optional=True)
        object    = self._get_step_attr(op, 'object',         optional=True)

        predicates = self._get_step_attr(op, 'predicates', list, optional=True)
        objects    = self._get_step_attr(op, 'objects',    list, optional=True)

        inverse    = self._get_step_attr(op, 'inverse',    str, optional=True)

        # The subject must be either provided directly or as implied subject
        if subject:
            if implied_subject:
                raise ProcessorException(f"Trying to redefine subject {implied_subject}")
        elif not implied_subject:
            raise ProcessorException(f"No subject defined in {str(op)}")

        # The predicate must be either provided directly, as implied predicate or
        #  as list of predicte sub-steps
        if predicate or predicates:
            # Defined directly
            if not (subject or implied_subject):
                raise ProcessorException(f"Trying to define predicate without subject {str(predicate or predicates)}")
            if implied_predicate:
                raise ProcessorException(f"Trying to redefine predicate {implied_predicate}")
        elif not implied_predicate:
            raise ProcessorException(f"No predicate defined in {str(op)}")

        # The object is optional (on subject level). 
        # In this case subject and predicate must be implied.
        # if it is provided, it must not redefine itself and subject and predicate must be available
        # directl or indirectly
        if object or objects:
            # Directly defined
            if not (subject or implied_subject):
                raise ProcessorException(f"Object defintion without subject in {str(op)}")
            if not (predicate or implied_predicate):
                raise ProcessorException(f"Object defintion without predicate in {str(op)}")
            if object and objects:
                raise ProcessorException(f"Trying to define single object and object list in {str(op)}")
            if predicates:
                raise ProcessorException(f"Trying to define object for multiple prtedicates at the same level in {str(op)}")
        # The availability of subject and predicate was already checked before

        if inverse and not predicate:
            raise ProcessorException(f"Inverse defintion not tied to predicate in {str(op)}")

        # Now lets define triplets
        if predicates:
            self._execute_sub_steps(predicates, implied_subject=subject)
        elif objects:
            self._execute_sub_steps(objects, implied_subject=(subject or implied_subject), implied_predicate=predicate, implied_inverse=inverse)
        elif object:
            self._add_triple(subject or implied_subject, predicate or implied_predicate, object, inverse=(inverse or implied_inverse))
        return (True, None)


    def _add_triple(self, subject, predicate, object, inverse=None):
        """ Adds a triple to the current graph including its inverse if provided. """
        self._triples.append( (self._expand_template(subject), self._expand_template(predicate), self._expand_template(str(object))))
        s = self._parse_node(subject)
        p = self._parse_node(predicate)
        o = self._parse_literal(object)
        self._graph.add( (s, p, o) )
        self._update_stats(added_triples=1)
        if self._showTriples:
            self._info(f"Added: {self._spo_lit(s)} {self._spo_lit(p)} {self._spo_lit(o)}")
        elif self._no_triples % 100 == 0:
            self._info(f"Triples added so far: {self._no_triples}")
        if inverse:
            i = self._parse_node(inverse)
            self._graph.add( (o, i, s) )
            self._update_stats(added_triples=1)
            if self._showTriples:
                self._info(f"Added: {self._spo_lit(o)} {self._spo_lit(i)} {self._spo_lit(s)}")
            elif self._no_triples % 100 == 0:
                self._info(f"Triples added so far: {self._no_triples}")



    def _spo_lit(self, spo):
        """ Converts a HTTP URL to an IRI using a namespace prefix if that is possible. """
        lit = str(spo)
        if lit.startswith("http"):
            for prefix, uri in self._prefixes.items():
                if lit.startswith(uri):
                    lit = f"{prefix}:{lit[len(uri):]}"
                    break
        return lit


    re_prefix_ref = re.compile(r'\s*([a-zA-Z][a-zA-Z0-9]*)\:([a-zA-Z][a-zA-Z0-9_\.\-/#]*)\s*')

    def _parse_node(self, n):
        """ Parses a subject or predicate literal and returns the according URIRef object for triple creation. """
        # h = n
        n = self._expand_template(n)
        # print(f"_parse_node {h} => {n}")
        m:re.Match = TextToTurtleProcessor.re_prefix_ref.match(n)
        if m:
            # A node that is a namespace prefix plus a member reference
            prefix = m.group(1)
            member = m.group(2)
            ns = self._prefixes.get(prefix)
            if ns is None:
                raise ProcessorException(f"Unknown prefix: {prefix}")
            node = getattr(ns, member)
            return node
        # Otherwise it is assumed that the node literal is a complete IRI of some sort
        # n = self._expand_template(n)
        node = URIRef(n)
        return node


    def _parse_literal(self, l):
        """ Parses an object literal and creates the according Literal object for triple creation. """
        if isinstance(l, Literal) or isinstance(l,URIRef):
            # Already a RDF type, so return as is
            return l
        if isinstance(l, dict):
            # Structured literal
            text   = self._get_step_attr(l, 'text',    str, optional=True)
            iri    = self._get_step_attr(l, 'iri',     str, optional=True)
            lang   = self._get_step_attr(l, 'lang',    str, optional=True)
            vint   = self._get_step_attr(l, 'integer', str, optional=True)
            vfloat = self._get_step_attr(l, 'float',   str, optional=True)
            vbool  = self._get_step_attr(l, 'bool',    str, optional=True)
            vdate  = self._get_step_attr(l, 'date',    str, optional=True)
            vdtime = self._get_step_attr(l, 'datetime',str, optional=True)
            format = self._get_step_attr(l, 'format',  str, optional=True)
            apply  = self._get_step_attr(l, 'apply',   str, optional=True)
            ndef = []
            for value, attr in [(text, 'text'), (iri, 'iri'), (vint, 'integer'), (vfloat, 'float'), (vbool, 'bool')]:
                if value is not None:
                    ndef.append(attr)
            if len(ndef) > 1:
                raise ProcessorException(f"Triple object {str(l)} has multiple value attributes: {', '.join(ndef)}")
            if len(ndef) < 1:
                raise ProcessorException(f"Triple object {str(l)} has no value attribute (expected one of text:, iri:, integer:, float:, bool:)")
            if vint:
                v = self._expand_template(vint)
                try:
                    i = int(v)
                    return Literal(i, datatype=XSD.integer)
                except ValueError:
                    raise ProcessorException(f"Object integer: argument is no int literal: {str(v)}")
            if vfloat:
                v = self._expand_template(vfloat)
                if v.strip() == "":
                    v = "nan"
                try:
                    f = float(v)
                    return Literal(f, datatype=XSD.float)
                except ValueError:
                    raise ProcessorException(f"Object float: argument is no float literal: {str(v)}")
            if vbool:
                v = self._expand_template(vbool)
                if v.strip() == "":
                    v = "False"
                try:
                    b = not (str(v).lower().strip() in ["false", "0", "0.0", "", "none"])
                    return Literal(b, datatype=XSD.boolean)
                except ValueError:
                    raise ProcessorException(f"Object bool: argument is no boolean literal: {str(v)}")
            if vdate:
                v = self._expand_template(vdate)
                if v.strip().lower() == "today":
                    d = date.today()
                else:
                    if format:
                        try:
                            d = datetime.strptime(v, format).date()
                            return Literal(d, datatype=XSD.date)
                        except ValueError:
                            raise ProcessorException(f"Object date: argument is no valid date for format {format}: {str(v)}")
                    else:
                        try:
                            d = date.fromisoformat(v)
                            return Literal(d, datatype=XSD.date)
                        except ValueError:
                            raise ProcessorException(f"Object date: argument is no valid ISO-format date: {str(v)}")
            if vdtime:
                v = self._expand_template(vdtime)
                if v.strip().lower() == "now":
                    d = datetime.now()
                else:
                    if format:
                        try:
                            d = datetime.strptime(v, format)
                            return Literal(d, datatype=XSD.dateTime)
                        except ValueError:
                            raise ProcessorException(f"Object datetime: argument is no valid date time for format {format}: {str(v)}")
                    else:
                        try:
                            d = datetime.strptime(v, format)
                            return Literal(d, datatype=XSD.dateTime)
                        except ValueError:
                            raise ProcessorException(f"Object datetime: argument is no valid date time: {str(v)}")
            if iri:
                iri = self._expand_template(iri)
                m:re.Match = TextToTurtleProcessor.re_prefix_ref.match(iri)
                if m:
                    prefix = m.group(1)
                    member = m.group(2)
                    ns = self._prefixes[prefix]
                    if ns is None:
                        raise ProcessorException(f"Unknown prefix: {prefix}")
                    node = getattr(ns, member)
                    return node
                # iri = self._expand_template(iri)
                return URIRef(iri)
            if text is not None:
                t = self._expand_template(text)
                if lang:
                    return Literal(t, lang=lang) #, datatype=XSD.string)
                return Literal(t) #, datatype=XSD.string)
            if apply is not None:
                text = self._expand_template(text)
                text = self._apply_mapping(apply, text)
                m = TextToTurtleProcessor.re_prefix_ref.match(text)
                if m:
                    # A node that is a namespace prefix plus a member reference
                    prefix = m.group(1)
                    member = m.group(2)
                    ns = self._prefixes.get(prefix)
                    if ns is None:
                        raise ProcessorException(f"Unknown prefix: {prefix}")
                    node = getattr(ns, member)
                    return node                    
            else:
                text = self._expand_template(text)
            if lang:
                return Literal(text, lang=lang)
            # return Literal(text)
            l = text
        elif isinstance(l, list):
            # Operation sequence, so take the value of the first successful one
            success, value = self._execute_seq(l, return_first_success=True)
            if not success:
                raise ProcessorException(f"Failed to provide an object value within {str(l)}")
            if isinstance(value,Literal) or isinstance(value,URIRef):
                # Literal and URIRef co-derive from str, so they must be checked before
                return value
            if not isinstance(value, str):
                # Something more specific was produced, so return that as is
                return value
            # Else subject the provide value to string literal interpretation
            l = value
        # Plain string based literal
        l = self._expand_template(l)
        try:
            i = int(l)
            return Literal(i, datatype=XSD.integer)
        except ValueError:
            pass # Wasn't an int
        try:
            f = float(l)
            return Literal(f, datatype=XSD.float)
        except ValueError:
            pass # Wasn't a float
        sl = l.strip()
        if sl in ['true', 'True', 'TRUE']:
            return Literal(True)
        if sl in ['false', 'False', 'FALSE']:
            return Literal(False)
        if l.startswith('^'):
            l = l[1:]
        # l = self._expand_template(l)
        return Literal(l)


    #### Common private helper methods ####

    def _execute_seq(self, plan:list, return_first_success:bool = False, stop_on_failure:bool = False) -> Tuple[bool,Any]:
        """ Executes a sequence of operations.

        By default each entry in the list is treated as operation specification which is tried
        to be executed. The result of the last operation is returned.
        In case return_first_success is True, the result of the first successful operation
        is returned. I case stop_on_failure is True, execution stops as soon as an opration is not
        successful. Then its result is returned.
        """
        cur_ltl = self._log_task_level
        try:
            self._log_task_level += 1
            for step in plan:
                if not isinstance(step, dict):
                    raise ProcessorException(f"Plan step {str(step)} is not a dict")
                op = None
                for keyword, meth in TextToTurtleProcessor.keyword_2_method.items():
                    if keyword in step:
                        op = meth
                        break
                if op is None:
                        raise ProcessorException(f"No operation key found in step: {str(step)}")      
                # Finally call the operaion handler method
                reply = op(self, step)
                if isinstance(reply,tuple):
                    success, value = reply
                else:
                    success = True
                    value   = reply
                if success and return_first_success:
                    return (True, value)
                if not success and stop_on_failure:
                    return (False, value)
        finally:
            self._log_task_level = cur_ltl
        return (success, value)
            

    def _execute_steps(self, plan:list):
        """ DEPRECATED. """
        for step in plan:
            if not isinstance(step, dict):
                raise ProcessorException(f"Plan step {str(step)} is not a dict")
            op = None
            for keyword, meth in TextToTurtleProcessor.keyword_2_method.items():
                if keyword in step:
                    op = meth
                    break
            if op is None:
                 raise ProcessorException(f"No operation key found in step: {str(step)}")      
            # Finally call the opetaion handler method
            op(self, step)
            

    def _execute_sub_steps(self, plan:list, **kwargs):
        """ Executes the list of sub-steps with the provided keyword areguments. """
        for step in plan:
            if not isinstance(step, dict):
                raise ProcessorException(f"Plan step {str(step)} is not a dict")
            op = None
            for keyword, meth in TextToTurtleProcessor.keyword_2_method.items():
                if keyword in step:
                    op = meth
                    break
            if op is None:
                 raise ProcessorException(f"No operation key found in step: {str(step)}")      
            # Finally call the opetaion handler method
            op(self, step, **kwargs)


    def _expand_template(self, template:str) -> str:
        """ Expands a text template by replacing all match- and variable references with their value. 
        
        It replaces all references of the form 

          @{<var-or-match-name>(.<match-group>)(:<formatting>)}

        with the actual value of that variable or match group. This is done as long as no more
        references are found. It is also possible to nest references. E.g. the nested reference

          @{material-@{row}:trim}

        is expanded by first replacing the value of the variable "row" iniside the outer reference, resulting
        in a reference like "@{material-3.1:trim} (assumingh the value of "row" is 3), which is then replaced
        with the value of group 1 of match "material-3". An finally the value is trimmed.
            
        The following formatiings are supported:
          - trim: Removed lading and trailing whitespaces
          - iri: Removes every character that is not a letter, digit or "-" or "_" with an underscore. Also the value is trimmed first.
          - norm: Normalizes whitespaces in the string, that means newlines, tabs and cariage returns are repaces with a blank.
        """
        templ_arg = template
        try:
            while True:
                opt = False
                m = re.search(r'@\{\??(([a-zA-Z0-9_-]+)\.)?((\d+)|([a-zA-Z][a-zA-Z0-9_-]*))(\:[a-zA-Z]+)?\}', template)
                if m is None:
                    break
                if m.group(0).startswith('@{?'):
                    opt = True
                id = m.group(2)
                if m.group(4):
                    gn = int(m.group(4))
                else:
                    gn = m.group(5)
                try:
                    if id:
                        # Named match refernce
                        if id in self._matches:
                            ext = self._matches[id].group(gn)
                        else:
                            if not opt:
                                raise ProcessorException(f"No match with id {id} found when looking at template {template}")
                            ext = ""
                    else:
                        v = str(self._var.get(gn))
                        if v is None:
                            if not opt:
                                ext = self._latest_match.group(gn)
                            else:
                                ext = ""
                        else:
                            ext = v
                except IndexError:
                    raise ProcessorException(f"Template expansion: No such group or variable {m.group(0)}")
                func_id = m.group(6)
                if func_id is not None:
                    func_id = func_id[1:]
                    # Tranformation function
                    if func_id == 'iri':
                        ext = re.sub(r'[^a-zA-Z0-9_-]', "_", ext.strip())
                    elif func_id == 'trim':
                        ext = ext.strip()
                    elif func_id == 'norm':
                        ext = html.unescape(re.sub(r'[\n\t\r]+', " ", ext.strip()))
                beg = m.start(0)
                end = m.end(0)
                template = template[:beg] + (ext or "") + template[end:]
        except Exception as ex:
            raise ProcessorException(f"Failed to expand template: {templ_arg}") from ex
        return template


    def _try_res (self, re_list:List[re.Pattern], text, start:int = 0) -> re.Match:
        """ Tries all the re.Pattern objects on the given text (re.search) and return the first match. """
        for reo in re_list:
            m = reo.search(text, start)
            if m:
                return m
        return None # No match found


    def _get_step_attr(self, step:dict, attr_name:str, expected_type=None, optional:bool=False):
        """ Gets a named attribute from the provided operation step dictionary. 
        
        If an attribute with the given name is not presnset and optional is not True,
        an acccording exception is thrown.
        If the expected type is not None, it is also checked if the actual value if of that type.
        If that is not the case, an exception is thrown.
        """
        attr = step.get(attr_name)
        if attr is None:
            if optional:
                return None
            raise ProcessorException (f"Expected step attribute '{attr_name}' missing")
        if expected_type is not None and not isinstance(attr, expected_type):
            raise ProcessorException (f"Processing step attribute '{attr_name}' has invalid type; expected {str(expected_type)}")
        return attr


    def _get_matching_attrs(self, step:dict, attr_name_re:str, expected_type=None, optional:bool=True):
        """ Returns all the attribute names in the operation-step dictionary that match the given name regular expression. """
        reo = re.compile(attr_name_re)
        attrs = {}
        for name, value in step.items():
            if reo.match(name):
                if expected_type is not None and not isinstance(value, expected_type):
                    raise ProcessorException (f"Processing step attribute '{name}' has invalid type; expected {str(expected_type)}")
                attrs[name] = value
        if not attrs and not optional:
            raise ProcessorException (f"Expected step attributes mathcing '{attr_name_re}' missing")
        return attrs


    def _update_stats(self, 
        match_inc:int = None, 
        match_len:int = None,
        added_triples: int = None
    ):
        """ Updates the match and/or generated triples statistics. """
        if match_inc is not None:
            self._no_matches += match_inc
        if match_len is not None:
            self._score += 1.0 - (1.0 / max(1,match_len))
            self._total_match_len += match_len
        if added_triples is not None:
            self._no_triples += added_triples


    flag_by_name = {
        'I':          re.IGNORECASE,
        'IGNORECASE': re.IGNORECASE,
        'L':          re.LOCALE,
        'LOCALE':     re.LOCALE,
        'A':          re.ASCII,
        'ASCII':      re.ASCII,
        'M':          re.MULTILINE,
        'MULTILINE':  re.MULTILINE,
        'S':          re.DOTALL,
        'DOTALL':     re.DOTALL,
        'X':          re.VERBOSE,
        'VERBOSE':    re.VERBOSE
    }

    def _resolve_flags(self, flags_src:Union[str,dict], init:int=0) -> int:
        """Computes a RE flags bit-vector (int) from a flags specification string.
        
        In case flags_src is a dictionary, the actual flags string is optained
        from its 'flags' item.
        """
        if isinstance(flags_src, dict):
            # A step dictionary
            flags_src = self._get_step_attr(flags_src, 'flags', str, optional=True)
        if flags_src is None:
            return init
        flags = init
        for name in re.split(r'\s+', flags_src):
            v = TextToTurtleProcessor.flag_by_name.get(name)
            if v is None:
                raise ProcessorException(f"Invalid RE flag: {name}")
            flags = flags | v
        return flags


    # Mapping of opetaion keywords to the according operation handler methods
    keyword_2_method = {
        'pass': nop,
        'any-of': use_any_of,
        'set': assign_var,
        'clear': clear_var,
        'append': append_to_list,
        'for-each': for_each_elem,
        'exec': exec_python_code,
        'if': cond_exec,
        'ifdef': ifdef_exec,
        'ifndef': ifndef_exec,
        'save-as': save_as,
        'procedure': def_procedure,
        'call': call_procedure,
        'replace': replace_pattern,
        'match': match_pattern,
        'match-every': match_every,
        'within': exec_within,
        'within-every': exec_within_every,
        'with': exec_with,
        'sequence-of': match_seq_of,
        'match-1': two_dimenisonal_match,
        'match-dimensions': multi_dimenisonal_match,
        'tag-dimension': tag_dimension,
        'break': break_dimension,
        'prefix': def_prefix,
        'mapping': def_mapping,
        'map': map_value,
        'select': query_graph,
        'subject': def_triple,
        'predicate': def_triple,
        'object': def_triple,
        'echo': echo,
        'desc': description,
        'dump': dump_text
    }
            
        
    #### Logging and Environment ####

    def _info(self, message):
        """ Logs a message with the INFO prefix."""
        self._log('INFO: ', message)


    def _warning(self, message):
        """ Logs a message with the WARNING prefix."""
        self._log('WARNING: ', message)


    def _log(self, prefix, message):
        """ Logs the message with the given prefix indented by the current log-task-level. """
        print(("  " * self._log_task_level) + prefix + message, file=self._log_stream)



    def is_aws_env(self):
        """Checks if the execution environment is an AWS environment.

        Checking for the existense of AWS_REGION or CONTAINER_ID environment variables
        does the trick for most AWS services using Python code
        """
        return os.environ.get("AWS_REGION") is not None or os.environ.get("CONTAINER_ID") is not None