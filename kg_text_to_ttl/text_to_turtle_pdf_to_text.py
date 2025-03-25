
from typing import Tuple, List, Dict, Set, Any
from functools import cmp_to_key


import sys
import bisect
import os.path

import fitz


# Flags: 'consolitated-guiding-lines', 'tables', 'cells'
_VISUALIZE_ = ['consolidated-guiding-lines', 'tables', 'cells', 'cells-and-blocks']
# Switch to turn visualization on and off 
_VIS_ON_ = False
# Tells if this is an AWS environment. If so, no visualization will happen at all
_IS_AWS_ENV_ = os.environ.get("AWS_REGION") is not None or os.environ.get("CONTAINER_ID") is not None

def _shall_visualize(what:str) -> bool:
    return _VIS_ON_ and what in _VISUALIZE_ and not _IS_AWS_ENV_



### Helper Functions ####

def inside(v:float, min_v:float, max_v:float, epsilon:float = 0):
    return min_v - epsilon <= v and v <= max_v + epsilon



class Rect:
    """ Rectangle defined by its upper left (x0,y0) and bottom right (x1,y1) coordinates.
    """
    
    def __init__(self, x0:float = 0, y0:float = 0, x1:float = 0, y1:float = 0) -> None:
        self.x0 = x0
        self.y0 = y0
        self.x1 = x1
        self.y1 = y1


    @classmethod
    def merge_neutral_rect(clz):
        return Rect(
            sys.float_info.max,
            sys.float_info.max,
            - sys.float_info.max,
            - sys.float_info.max,
        )


    def is_empty(self) -> bool:
        return self.x0 >= self.x1 or self.y0 >=self.y1
    
    empty = property(is_empty, doc="Attribute telling if the rectangle is empty.")


    def __str__(self) -> str:
        if self.is_empty():
            return f"[: {self.x0:3f}, {self.y0:3f} empty :]"
        return f"{self.x0:.3f}, {self.y0:.3f} {self.x1 - self.x0:.3f}x{self.y1 - self.y0:.3f}"


    def copy(self) -> 'Rect':
        return Rect (self.x0, self.y0, self.x1, self.y1)
    

    def _cx(self):
        return (self.x0 + self.x1) / 2
    
    cx = property(_cx, doc="Center x coordinate")
            

    def _cy(self):
        return (self.y0 + self.y1) / 2
    
    cy = property(_cy, doc="Center y coordinate")


    def _w(self):
        return self.x1 - self.x0
    
    width = property(_w, doc="Width of the rectangle")
            

    def _h(self):
        return self.y1 - self.y0
    
    height = property(_h, doc="Height of the rectangle")
            

    def merge(self, other):
        """ Merges this rectangle with other one (union onto itself). """
        self.x0 = min(self.x0, other.x0)
        self.y0 = min(self.y0, other.y0)
        self.x1 = max(self.x1, other.x1)
        self.y1 = max(self.y1, other.y1)


    def union(self, other) -> 'Rect':
        """ Returns a new rectangle that covers this and the other rectangle. """
        return Rect (
            min(self.x0, other.x0),
            min(self.y0, other.y0),
            max(self.x1, other.x1),
            max(self.y1, other.y1)
        )
    

    def intersection(self, other) -> 'Rect':
        """ Returns a new rectangle that is the interscation of this one and the other one."""
        return Rect (
            max(self.x0, other.x0),
            max(self.y0, other.y0),
            min(self.x1, other.x1),
            min(self.y1, other.y1)
        )
    

    def inset(self, amount:float) -> 'Rect':
        """ Insets (shrinks) the rectangle on each side (towards the center) by the given amount. """
        return Rect (
            self.x0 + amount,
            self.y0 + amount,
            self.x1 - amount,
            self.y1 - amount
        )


    def extend(self, amount:float) -> 'Rect':
        return self.inset(- amount)


    def area(self) -> float:
        if self.x0 >= self.x1 or self.y0 >= self.y1:
            return 0
        return (self.x1 - self.x0) * (self.y1 - self.y0)
    

    def overlaps(self, other:'Rect') -> bool:
        return other.x0 <= self.x1 and other.x1 >= self.x0 and \
               other.y0 <= self.y1 and other.y1 >= self.y0


    def epsilon_overlaps(self, other:'Rect', epsilon:float) -> bool:
        return other.x0 <= self.x1 + epsilon and other.x1 >= self.x0 - epsilon and \
               other.y0 <= self.y1 + epsilon and other.y1 >= self.y0 - epsilon


    def contains(self, other:'Rect') -> bool:
        return other.x0 >= self.x0 and other.x1 <= self.x1 and \
               other.y0 >= self.y0 and other.y1 <= self.y1


    def overlaps_horizontally(self, other:'Rect') -> bool:
        return other.x0 <= self.x1 and other.x1 >= self.x0


    def overlaps_vertically(self, other:'Rect') -> bool:
        return other.y0 <= self.y1 and other.y1 >= self.y0


    def covers_horizontally(self, other:'Rect') -> bool:
        return other.x0 >= self.x0 and other.x1 <= self.x1


    def covers_vertically(self, other:'Rect') -> bool:
        return other.y0 >= self.y0 and other.y1 <= self.y1


def rect_x0(r:Rect):
    return r.x0

def rect_y0(r:Rect):
    return r.y0



class PdfElement:
    """ Object that represents a part of a PDF page inside a rectangular area and that 
        can be rendered into the XHTML image. 
    
    Abstract class that just provides the bounds Rect and rendering methods.
    """

    def __init__(self, bounds:Rect=None) -> None:
        self.bounds:Rect = bounds
        

    def render(self, image:List[str]):
        """ Render the HTML representation of this object by appending strings to the image. """
        pass


class Block(PdfElement):
    """ A block of text inside a PDF page. """
    
    def __init__(self, bdef:Tuple=None, bounds:Rect = None) -> None:
        if bdef:
            x0, y0, x1, y1, lines, _, _, _ = bdef
            bounds:Rect = Rect(x0, y0, x1, y1)
            super().__init__(bounds)
            self.text:str    = lines
        else:
            super().__init__(bounds)
            self.text:str    = None


    def __str__(self) -> str:
        return f"[| '{self.text}': {self.bounds} |]"
    

    def render(self, image: List[str]):
        image.append(f"<p>{self.text}</p>\n")

    
    
alignment_delta = 1e-4

def aligned(a, b):
    return abs(a - b) <= alignment_delta


def compare_element_pos(a:PdfElement, b:PdfElement) -> bool:
    if aligned(a.bounds.y0, b.bounds.y0):
        return a.bounds.x0 - b.bounds.x0
    return a.bounds.y0 - b.bounds.y0


def on_same_row(a:Block, b:Block, same_line_block_overlap_fraction:float) -> bool:
    """ Checks if two blocks are on the same row by looking and their vertical overlap. """
    ov_y0 = max(a.bounds.y0, b.bounds.y0)
    ov_y1 = min(a.bounds.y1, b.bounds.y1)
    if ov_y0 >= ov_y1: 
        return False # No overlap at all
    ov_size = ov_y1 - ov_y0
    # return max(a.bounds.height / ov_size, b.bounds.height / ov_size) >= same_line_block_overlap_fraction
    return max(ov_size / a.bounds.height, ov_size / b.bounds.height) >= same_line_block_overlap_fraction




class Region (PdfElement):
    """ PDF Element that might contain other elements.
    
    Also table cells are regions.
    """

    def __init__(self, bounds:Rect) -> None:
        super().__init__(bounds=bounds)
        self.blocks:List[PdfElement] = []



    def __str__(self) -> str:
        bt = [b.text for b in self.blocks]
        bs = " | ".join(bt) 
        return f"[/ {self.bounds}: {bs} /]"


    def add(self, block:Block):
        self.bounds.merge(block.bounds)
        self.blocks.append(block)


    def merge(self, other:'Region'):
        self.bounds.merge(other.bounds)
        self.blocks.extend(other.blocks)


    def render(self, image: List[str]):
        image.append(f"<p>")
        for e in self.blocks:
            e.render(image)
        image.append(f"</p>\n")


class Table(PdfElement):
    """ Table defined by sets of horizontal and vertival lines and the cells inside that grid. """

    def __init__(self, h_lines:List[Rect]=None, v_lines:List[Rect] = None) -> None:
        super().__init__(Rect.merge_neutral_rect())
        self.h_lines = h_lines or []
        self.v_lines = v_lines or []
        # for h_l in self.h_lines:
        #     self.bounds.merge(h_l)
        # for v_l in self.v_lines:
        #     self.bounds.merge(v_l)
        for h_l in self.h_lines:
            self.bounds.y0 = min(self.bounds.y0, h_l.y0)
            self.bounds.y1 = max(self.bounds.y1, h_l.y1)
        for v_l in self.v_lines:
            self.bounds.x0 = min(self.bounds.x0, v_l.x0)
            self.bounds.x1 = max(self.bounds.x1, v_l.x1)
        self.cells:List[List[Region]] = []


    def render(self, image: List[str]):
        image.append("\n<table>\n")
        for row in self.cells:
            image.append("<tr>\n")
            for col in row:
                image.append("<td>")
                for blk in col.blocks:
                    blk.render(image)
                image.append("</td>")
            image.append("</tr>\n")
        image.append("</table>\n\n")



class PdfTableRecognizer:
    """ Identifies tables inside PDF pages by searching for grid lines that form a table.
    
    In case one or more tables are found, text blocks are mapped to the implied table cells.
    Text blocks that are not inside a table are still consolidated and handled as flow of paragraphs.

    Identifying tables is a multi-step process:

    1) Guiding lines are identfied. This are PDF drawings that have a predominantly 
       horizontal or vertical oriented line-like gestalt
    2) Guiding lines are consolidated - that means connected or very close line segments are joined
       together
    3) Table borders are identified. That are guiding lines that "terminate" guiding lines
       of the opposite orientation
    4) Tables are identfied. That are grids of guiding lines that are terminatzed by
       table borders   
    5) Text blocks are mapped to table cells
    6) Cell contents is consolidated, that means text blocks that form lines or
       paragraphs are joined together   
    7) The same is done for the remaining text blocks
    
    Finally the PDF elements (blocks, regions, tables) are regndered into an XHTML 
    text image.
    
    """

    def __init__(
        self
    )-> None:
        self.blocks:List[Block] = []
        self.image:List[str] = []
        self.page:int = 0
        self.current_page = None
        # Identified lines in a page
        self.horizontal_lines:List[Rect] = []
        self.vertical_lines:List[Rect] = []
        # Table outer border candidates
        self.top_borders:List[Rect] = []
        self.bot_borders:List[Rect] = []
        self.lft_borders:List[Rect] = []
        self.rgt_borders:List[Rect] = []
        # The identified tables
        self.tables:List[Table] = []
        self.doc_image:List[str] = []
        # Tuning parameters
        self.p_min_line_length = 5 # 10
        self.p_max_line_width = 1.2
        self.p_min_guideline_length = 16 # 24 # 4-5 characters wide / high
        self.p_max_line_offset = 0.5
        self.p_max_join_distance = 1.5 # 1
        self.p_max_border_dist = 2 # 1.2
        self.p_border_threshold = 4
        self.p_table_min_h_lines = 3
        self.p_table_min_v_lines = 4
        self.p_suffcient_cell_overlap_fraction = 0.9
        self.p_min_cell_overlap_fraction = 0.5
        self.p_min_line_join_height_dist = 0.2 # Fraction of line height
        self.p_max_line_join_height_offset = 0.4 # Fraction of line height
        self.p_text_col_epsilon = 1
        self.p_max_text_col_width_join_ratio = 4
        # Mappig parameters
        self.p_page_div = True # if True wraps a <div class="page" pageno="x">...</div> around the page contents
        self.p_same_line_block_overlap_fraction = 0.87 # Fraction by which two blocks must vertically overlap to be considerd at the same line
        self.p_narrower_col_extension_fraction = 0.25
        self.p_wider_col_extension_fraction = 0.5

        self.a_max_skipped_h_line_len = 0
        self.a_max_skipped_v_line_len = 0




    def process_doc(self, doc:fitz.Document, pages:List[int] = None):
        def _on_same_row_compare(a:Block, b:Block):
            if on_same_row(a, b, self.p_same_line_block_overlap_fraction):
                return a.bounds.x0 - b.bounds.x0
            return a.bounds.y0 - b.bounds.y0

        for page in doc.pages():
            self.current_page = page

            if pages is None or self.page in pages:
                print(f"PAGE {self.page} ----------------------------------------------------------")
                if self.p_page_div:
                    self.doc_image.append(f'<div class="page" pageno="{self.page + 1}">\n')
                self.doc_image.append(f"<!-- --------------------------- {self.page + 1} --------------------------- -->\n")

                block_tuples = page.get_textpage().extractWORDS()
                self.blocks = []
                for bt in block_tuples:
                    self.blocks.append(Block(bt))
                
                self.extract_guiding_lines()

                if _shall_visualize('cells-and-blocks'):
                    crt = []
                    blk = [b.bounds for b in self.blocks]
                    blk.extend(self.horizontal_lines)
                    blk.extend(self.vertical_lines)
                    visualize_rects_and_blocks(crt, blk, title=f"Page {self.page + 1}: Blocks", rect_color='magenta', block_outline='red', inset=1)

                self.consolidate_guiding_lines()
                # for l in self.horizontal_lines:
                #     print(f"HL: {l}")
                # for l in self.vertical_lines:
                #     print(f"VL: {l}")

                if _shall_visualize('consolidated-guiding-lines'):
                    visualize_rect_lines(self.horizontal_lines, self.vertical_lines, title=f"Page {self.page + 1}: Consolidated Guiding Lines")
                # dr = page.get_drawings()
                # print(dr)
                self.identify_table_borders()
                self.identify_tables()

                if _shall_visualize('tables'):
                    trl = []
                    for t in self.tables:
                        trl.append(t.h_lines)
                        trl.append(t.v_lines)
                    visualize_rect_lines(*trl, title=f"Page {self.page + 1}: Tables", line_color='red')

                self.grap_cells()

                if _shall_visualize('cells'):
                    crt = []
                    for t in self.tables:
                        for row in t.cells:
                            for reg in row:
                                crt.append(reg.bounds)
                    visualize_rects(crt, title=f"Page {self.page + 1}: Cells", rect_color='magenta')
                if _shall_visualize('cells-and-blocks'):
                    crt = []
                    blk = [b.bounds for b in self.blocks]
                    for t in self.tables:
                        for row in t.cells:
                            for reg in row:
                                crt.append(reg.bounds)
                                for b in reg.blocks:
                                    blk.append(b.bounds)
                    visualize_rects_and_blocks(crt, blk, title=f"Page {self.page + 1}: Cells and Text Blocks", rect_color='magenta', inset=1)

                self.consolidate_cell_contents()

                # Consolidate the remaining text
                self.blocks = self.join_line_blocks(self.blocks)

                # Join back the tables and sort again
                self.blocks.extend(self.tables)
                # self.blocks = sorted(self.blocks, key=cmp_to_key(compare_element_pos))
                self.blocks = sorted(self.blocks, key=cmp_to_key(_on_same_row_compare))

                # Finally generate the image
                self.image = []
                for b in self.blocks:
                    b.render(self.image)

                # And now dum it
                img = self.get_image()
                self.doc_image.append(img)

                if self.p_page_div:
                    self.doc_image.append(f'</div>\n')

                # print(img)
                # print("------------------------------------------------------------------")
                
            self.page += 1


    def join_line_blocks(self, blocks:List[Block]):
        blocks = self.join_top_aligned_line_blocks(blocks)
        blocks = self.join_x_adjacent_line_blocks(blocks)
        blocks = self.join_y_adjacent_line_blocks(blocks)
        return blocks


    def join_same_row_line_blocks(self, blocks:List[Block]):
        """Joins blocks that a largely located at the same row."""
        def _on_same_row(a:Block, b:Block) -> bool:
            ov_y0 = max(a.bounds.y0, b.bounds.y0)
            ov_y1 = min(a.bounds.y1, b.bounds.y1)
            if ov_y0 >= ov_y1: 
                return False # No overlap at all
            ov_size = ov_y1 - ov_y0
            return max(a.bounds.height / ov_size, b.bounds.height / ov_size) >= self.p_same_line_block_overlap_fraction

        def _cmp_block_pos(a:Block, b:Block) -> bool:
            if _on_same_row(a, b):
                return a.bounds.x0 - b.bounds.x0
            return a.bounds.y0 - b.bounds.y0

        sorted_blocks = sorted(blocks, key=cmp_to_key(_cmp_block_pos))

        joined = []
        pred:Block = None
        for b in sorted_blocks:
            if pred is not None:
                if self.is_top_aligned_next_word(pred, b):
                    j = Block()
                    j.bounds = pred.bounds.union(b.bounds)
                    j.text   = f"{pred.text} {b.text}"
                    pred = j
                    # print (f"BJ: {pred}")
                else:
                    joined.append(pred)
                    pred = b
            else:
                pred = b
        if pred:
            joined.append(pred)
        return joined


    def join_top_aligned_line_blocks(self, blocks:List[Block]):
        """Joins blocks that are top-aligned and that are very close to each other (less than the width of 1.5 characters)."""
        def _cmp_block_pos(a:Block, b:Block) -> bool:
            if aligned(a.bounds.y0, b.bounds.y0):
                return a.bounds.x0 - b.bounds.x0
            return a.bounds.y0 - b.bounds.y0
        sorted_blocks = sorted(blocks, key=cmp_to_key(_cmp_block_pos))

        joined = []
        pred:Block = None
        for b in sorted_blocks:
            if pred is not None:
                if self.is_top_aligned_next_word(pred, b):
                    j = Block()
                    j.bounds = pred.bounds.union(b.bounds)
                    j.text   = f"{pred.text} {b.text}"
                    pred = j
                    # print (f"BJ: {pred}")
                else:
                    joined.append(pred)
                    pred = b
            else:
                pred = b
        if pred:
            joined.append(pred)
        return joined


    def is_top_aligned_next_word(self, block:Block, next:Block) -> bool:
        if not aligned(block.bounds.y0, next.bounds.y0):
            return False
        avg_char_width = (block.bounds.x1 - block.bounds.x0) / len(block.text)
        return next.bounds.x0 > block.bounds.x1 and next.bounds.x0 < block.bounds.x1 + avg_char_width * 1.5


    def join_x_adjacent_line_blocks(self, blocks:List[Block]):
        """Joins blocks close adjacent to each other on the x-axis (less than the width of 1.5 characters)."""
        def _cmp_block_x_pos(a:Block, b:Block) -> bool:
            return a.bounds.x0 - b.bounds.x0
        blocks = sorted(blocks, key=cmp_to_key(_cmp_block_x_pos))

        # No for-in-lopp as the blocks array is modified inside the loop
        i = 0
        while i < len(blocks) - 1:
            b = blocks[i]
            n = i + 1
            avg_char_width = (b.bounds.x1 - b.bounds.x0) / len(b.text)
            # Skip all blocks that overlap (as they might be somewhere up or down yet on a similar x-pos)
            while n < len(blocks) and blocks[n].bounds.x0 < b.bounds.x1:
                n += 1
            while n < len(blocks) and blocks[n].bounds.x0 < b.bounds.x1 + 1.5 * avg_char_width:
                if blocks[n].bounds.y0 <= b.bounds.cy and blocks[n].bounds.y1 >= b.bounds.cy:
                    # A block that follows b closely and overlap the center-y of b
                    j = Block()
                    j.bounds = b.bounds.union(blocks[n].bounds)
                    j.text   = f"{b.text} {blocks[n].text}"
                    blocks[i] = j
                    del blocks[n]
                    b = j
                    # print (f"XJ: {j}")
                else:
                    n += 1
            i += 1
            
        return blocks


    def join_y_adjacent_line_blocks(self, blocks:List[Block]):
        """Joins blocks close adjacent to each other on the y-axis that are inside the same horizontal region."""
        def _cmp_block_y_pos(a:Block, b:Block) -> bool:
            return a.bounds.y0 - b.bounds.y0
        blocks = sorted(blocks, key=cmp_to_key(_cmp_block_y_pos))

        # No for-in-lopp as the blocks array is modified inside the loop
        i = 0
        while i < len(blocks) - 1:
            b = blocks[i]
            n = i + 1
            line_height = b.bounds.y1 - b.bounds.y0
            # Skip all blocks that overlap (as they might be somewhere left or right yet on a similar y-pos)
            while n < len(blocks) and blocks[n].bounds.y0 < b.bounds.y1 - line_height * self.p_min_line_join_height_dist:
                n += 1
            while n < len(blocks) and blocks[n].bounds.y0 < b.bounds.y1 + line_height * self.p_max_line_join_height_offset:
                if (b.bounds.covers_horizontally(blocks[n].bounds) or blocks[n].bounds.covers_horizontally(b.bounds)) and \
                   self.is_column_aligned_extension_2(b.bounds, blocks[n].bounds):
                    # A block that follows closely below b and is horizontally contained (or vice versa)
                    j = Block()
                    j.bounds = b.bounds.union(blocks[n].bounds)
                    j.text   = f"{b.text}<br/>{blocks[n].text}"
                    blocks[i] = j
                    del blocks[n]
                    b = j
                    # print (f"YJ: {j}")
                else:
                    n += 1
            i += 1
            
        return blocks


    def is_column_aligned_extension_2(self, top_part:Rect, bot_part:Rect):
        """ Heuristic test that checks if a rectangle bot_part is an extension of the column top_part.

        Revised 2nd version.

        That seems to be the case if one of this condition apply:
            If the top part is wider than the bottom one:
                - Their size ratio is larger than p_narrower_col_extension_fraction
            Otherwise:
                - Their size ratio is larger than self.p_wider_col_extension_fraction
        Two differnz size ratios are used because the case that a bottom extension is much smaller
        than the column above is much more likel to happen at the end of a text column.

        As with all heuristics, this might not be correct in all cases.
        """
        if top_part.width >= bot_part.width:
            # The column top part is wider than the bottom extension. 
            # Something like this:
            # 
            # -------------  -------------  -------------
            # ----             ---------           ------
            # 
            # In this case it is enough that the bottom part is left or right aligned
            # or has is at least p_narrower_col_extension_fraction as wide as the top part
            if abs(bot_part.x0 - top_part.x0) < self.p_text_col_epsilon or \
               abs(bot_part.x1 - top_part.x1) < self.p_text_col_epsilon :
                return True # left or right aligned
            size_ratio = bot_part.width / top_part.width
            return size_ratio >= self.p_narrower_col_extension_fraction
        else:
            # The column top part is narrower than the bottom extension. 
            # Something like this:
            # 
            # --------         ---------           ------
            # -------------  -------------  -------------
            # 
            # In this case it is the top part must at least be
            # p_wider_col_extension_fraction as wide as the bottom part
            size_ratio = top_part.width / bot_part.width
            return size_ratio >= self.p_wider_col_extension_fraction
    

    def is_column_aligned_extension(self, column:Rect, ext:Rect):
        """ Heuristic test that checks if a rectangle ext is an extension of the column rect.

        That seems to be the case if one of this condition apply:
        - Both rects are left-aligned
        - Both rects are right-aligned
        - The size relation (wider / narrower) is below 4

        As with all heuristics, this might not be correct in all cases.
        """
        # return True
        # epsilon = 1
        if abs(ext.x0 - column.x0) < self.p_text_col_epsilon or \
           abs(ext.y1 - column.y1) < self.p_text_col_epsilon :
            return True
        size_ratio = column.width / ext.width
        if size_ratio < 1:
            size_ratio = 1 / size_ratio
        return size_ratio <= self.p_max_text_col_width_join_ratio
    

    def extract_guiding_lines(self):
        self.horizontal_lines = []
        self.vertical_lines = []
        paths = self.current_page.get_drawings()
        for path in paths:
            items =  path["items"] #path.items
            # A path is made up of items, so lines, rects, curves, etc.
            # Use a heurstic here to extract guiding / table lines / cell boundaries
            if len(items) == 1 and len(items[0]) == 3 and items[0][0] == 're':
                # Item of the form ('re', Rect, 1)
                # That could be a cell border line rectangle (line segment represented as very thin rectangle)
                r = items[0][1]
                rx0 = r.x0
                ry0 = r.y0
                rx1 = r.x1
                ry1 = r.y1
                if ry1 - ry0 <= self.p_max_line_width:
                    # A very thin rectangle, so add it to the 
                    if rx1 - rx0 >= self.p_min_line_length:
                        self.horizontal_lines.append(Rect(rx0, ry0, rx1, ry1))
                    else:
                        self.a_max_skipped_h_line_len = max(rx1 - rx0, self.a_max_skipped_h_line_len)
                        # print(f"Skipped H-Line W={rx1 - rx0}")
                elif rx1 - rx0 < self.p_max_line_width:
                    if ry1 - ry0 >= self.p_min_line_length:
                        self.vertical_lines.append(Rect(rx0, ry0, rx1, ry1))
                    else:
                        self.a_max_skipped_v_line_len = max(ry1 - ry0, self.a_max_skipped_v_line_len)
                        # print(f"Skipped V-Line W={ry1 - ry0}")


    def consolidate_guiding_lines(self):
        # min_line_length = 24 # 4-5 characters wide / high
        self.join_horizontal_line_segments()
        self.join_vertical_line_segments()
        self.horizontal_lines = list(filter(lambda r: r.width  > self.p_min_guideline_length, self.horizontal_lines))
        self.vertical_lines   = list(filter(lambda r: r.height > self.p_min_guideline_length, self.vertical_lines))


    def join_horizontal_line_segments(self):
        # max_line_offset = 0.5
        # max_join_distance = 1

        def cmp_line_rects(a:Rect, b:Rect):
            if abs(a.cy - b.cy) <= self.p_max_line_offset:
                return a.x0 - b.x0
            return a.y0 - b.y0
        
        hl_by_y_x:List[Rect] = sorted(self.horizontal_lines, key=cmp_to_key(cmp_line_rects))

        joined:List[Rect] = []
        pred:Rect = None
        for hl in hl_by_y_x:
            if pred is None:
                pred = hl
            elif abs(hl.cy - pred.cy) <= self.p_max_line_offset: # On the same y-level
                if abs(hl.x0 - pred.x1) <= self.p_max_join_distance: # Ends connect
                    pred.merge(hl)
                else:
                    joined.append(pred)
                    pred = hl
            else:
                joined.append(pred)
                pred = hl
        if pred: 
            joined.append(pred)
        self.horizontal_lines = joined


    def join_vertical_line_segments(self):
        # max_line_offset = 0.5
        # max_join_distance = 1

        def cmp_line_rects(a:Rect, b:Rect):
            if abs(a.cx - b.cx) <= self.p_max_line_offset:
                return a.y0 - b.y0
            return a.x0 - b.x0
        
        vl_by_x_y:List[Rect] = sorted(self.vertical_lines, key=cmp_to_key(cmp_line_rects))

        joined:List[Rect] = []
        pred:Rect = None
        for hl in vl_by_x_y:
            if pred is None:
                pred = hl
            elif abs(hl.cx - pred.cx) <= self.p_max_line_offset: # On the same y-level
                if abs(hl.y0 - pred.y1) <= self.p_max_join_distance: # Ends connect
                    pred.merge(hl)
                else:
                    joined.append(pred)
                    pred = hl
            else:
                joined.append(pred)
                pred = hl
        if pred: 
            joined.append(pred)
        self.vertical_lines = joined


    def identify_table_borders(self):
        # max_border_dist = 1.2
        # border_threshold = 4
        
        self.top_borders = []
        self.bot_borders = []
        for hl in self.horizontal_lines:
            top_ends = 0
            bot_ends = 0
            for vl in self.vertical_lines:
                if abs(hl.cy - vl.y0) <= self.p_max_border_dist:
                    top_ends += 1
                if abs(hl.cy - vl.y1) <= self.p_max_border_dist:
                    bot_ends += 1
            if top_ends >= self.p_border_threshold:
                self.top_borders.append(hl)
            if bot_ends >= self.p_border_threshold:
                self.bot_borders.append(hl)

        self.lft_borders = []
        self.rgt_borders = []
        for vl in self.vertical_lines:
            lft_ends = 0
            rgt_ends = 0
            for hl in self.horizontal_lines:
                if abs(vl.cx - hl.x0) <= self.p_max_border_dist:
                    lft_ends += 1
                if abs(vl.cx - hl.x1) <= self.p_max_border_dist:
                    rgt_ends += 1
            if lft_ends >= self.p_border_threshold:
                self.lft_borders.append(vl)
            if rgt_ends >= self.p_border_threshold:
                self.rgt_borders.append(vl)



    def identify_tables(self) -> List[Table]:   
        # max_border_dist = 1.2
        self.tables = []
        for t_bd in self.top_borders:
            for l_bd in self.lft_borders:
                if t_bd.epsilon_overlaps(l_bd, self.p_max_border_dist):
                    for r_bd in self.rgt_borders:
                        if t_bd.epsilon_overlaps(r_bd, self.p_max_border_dist):
                            for b_bd in self.bot_borders:
                                if l_bd.epsilon_overlaps(b_bd, self.p_max_border_dist) and r_bd.epsilon_overlaps(b_bd, self.p_max_border_dist):
                                    # Found all 4 borders
                                    t_area = Rect(l_bd.cx, t_bd.cy, r_bd.cx, b_bd.cy)
                                    # Get the grid lines
                                    h_lines = list(filter(lambda h_l: t_area.epsilon_overlaps(h_l, self.p_max_border_dist), self.horizontal_lines))
                                    v_lines = list(filter(lambda v_l: t_area.epsilon_overlaps(v_l, self.p_max_border_dist), self.vertical_lines))
                                    # h_lines = list(filter(lambda h_l: self.is_h_gridline(h_l, t_bd, b_bd, l_bd, r_bd, min_dist), self.horizontal_lines))
                                    # v_lines = list(filter(lambda v_l: self.is_v_gridline(v_l, t_bd, b_bd, l_bd, r_bd, min_dist), self.vertical_lines))
                                    if len(v_lines) < self.p_table_min_v_lines or len(h_lines) < self.p_table_min_h_lines:
                                        # Table with too few rows or columns - so ignore that
                                        continue
                                    # New check if that table area overlaps with an existing one
                                    # if so - replace the table if this area is bigger
                                    overlap = False
                                    replace = None
                                    for ti, other in enumerate(self.tables):
                                        if other.bounds.overlaps(t_area):
                                            overlap = True
                                            if t_area.area() > other.bounds.area():
                                                replace = ti
                                            break
                                    if not overlap or replace is not None: # Could be 0!
                                        tab = Table(h_lines, v_lines)
                                        if replace is None:
                                            self.tables.append(tab)
                                        else:
                                            self.tables[replace] = tab
        return self.tables


    def is_h_gridline(self, r:Rect, t_bd:Rect, b_bd:Rect, l_bd:Rect, r_bd:Rect, min_dist:float):
        return inside(r.cy, t_bd.cy, b_bd.cy, min_dist) and \
               abs(r.x0 - l_bd.cx) < min_dist and abs(r.x1 - r_bd.cx) < min_dist


    def is_v_gridline(self, r:Rect, t_bd:Rect, b_bd:Rect, l_bd:Rect, r_bd:Rect, min_dist:float):
        return inside(r.cx, l_bd.cx, r_bd.cx, min_dist) and \
               abs(r.y0 - t_bd.cy) < min_dist and abs(r.y1 - b_bd.cy) < min_dist


    def grap_cells(self):
        for t in self.tables:
            self.grap_cells_blocks(t)


    def grap_cells_blocks(self, table:Table):
        no_rows = len(table.h_lines) - 1
        no_cols = len(table.v_lines) - 1
        h_lines = sorted(table.h_lines, key=rect_y0)
        v_lines = sorted(table.v_lines, key=rect_x0)

        # rows = []
        # for h_l0, h_l1 in zip(h_lines[:-1], h_lines[1:]):
        #     row = []
        #     for v_l0, v_l1 in zip(v_lines[:-1], v_lines[1:]):
        #         cell = Region(Rect(v_l0.cx, h_l0.cy, v_l1.cx, h_l1.cy))
        #         row.append(cell)
        #     rows.append(row)
        rows = self.identify_cell_regions(table)

        ext_bounds = table.bounds.extend(1.2)
        # print(f"======================================")
        # print(f"TABLE {ext_bounds}")
        # for rz in rows:
        #     for cz in rz:
        #         print(f"{cz.bounds},  ", end='')
        #     print('')
        # print(f"======================================")

        remaining = []
        for b in self.blocks:
            if ext_bounds.contains(b.bounds):
                # Block potentially inside table
                ba = b.bounds.area()
                # print(f"B: {b.bounds} A={ba}  T={b.text}")

                ri = bisect.bisect_left(h_lines, b.bounds.y0, key=rect_y0)
                if ri >= len(rows) or (ri > 0 and b.bounds.y0 < rows[ri][0].bounds.y0):
                    ri -= 1 # Bisect insert pos is behind overlapping cell
                # print(f"RI: {ri}")
                ci = bisect.bisect_left(v_lines, b.bounds.x0, key=rect_x0)
                if ci >= len(rows[ri]) or (ci > 0 and b.bounds.x0 < rows[ri][ci].bounds.x0):
                    ci -= 1 # Bisect insert pos is behind overlapping cell
                # print(f"CI: {ci}")
                
                cell:Region = rows[ri][ci]
                itsc = cell.bounds.intersection(b.bounds).area()
                # print(f"C[{ri}][{ci}]: {cell.bounds} IA={itsc}")
                if itsc < ba * self.p_suffcient_cell_overlap_fraction:
                    # There's not a too great overlap with this cell, so try
                    # sourrounding cells as well. This could happen if a block
                    # starts in one cell but mainly covers an adjacent one
                    for rd in range(max(0, ri - 1), min(no_rows, ri + 1 + 1)):
                        for cd in range(max(0, ci - 1), min(no_cols, ci + 1 + 1)):
                            c:Region = rows[rd][cd]
                            a = c.bounds.intersection(b.bounds).area()
                            # print(f"D[{rd}][{cd}]: {c.bounds} IA={a}")
                            if a > itsc:
                                cell = c
                                itsc = a
                if itsc >= ba * self.p_min_cell_overlap_fraction:
                    # Block it as least half inside the table, so grab it
                    cell.blocks.append(b)
                else:
                    remaining.append(b)
            else:
                remaining.append(b)

        table.cells = rows
        self.blocks = remaining


    def identify_cell_regions(self, table:Table):
        """ Identifies the cells of a table.
        
        This method also detects cells that span several rows (verrtical merge).
        
        The returned list of list of cell regions still contains exactly no-rows * no-colls elements,
        yet if a cell spans multiple rows, the according region object is used in all covered cell positions.

        TODO: Add support for cells that span multiple columns.

        :param table: The table for which to identify their cells.
        :retruns: A list of lists of Region instances.
        """
        h_lines = sorted(table.h_lines, key=rect_y0)
        v_lines = sorted(table.v_lines, key=rect_x0)

        rows = []
        for h_l0, h_l1 in zip(h_lines[:-1], h_lines[1:]):
            row = []
            for v_l0, v_l1 in zip(v_lines[:-1], v_lines[1:]):
                cell_rect = Rect(v_l0.cx, h_l0.cy, v_l1.cx, h_l1.cy)
                cell_rect_core = cell_rect.inset(self.p_max_join_distance)
                # Check if the top border of the cell rect is actually
                # bordered by the top line h_l0 (or is the first top line)
                if len(rows) == 0 or h_l0.covers_horizontally(cell_rect_core):
                    # This is the case,  create a new region for that cell
                    cell = Region(Rect(v_l0.cx, h_l0.cy, v_l1.cx, h_l1.cy))
                else:
                    # A cell covering multiple rows (vertical cell merge).
                    # So reuse the one from the row above at this hor. position
                    cell = rows[-1][len(row)]
                    # and enhace its bounds
                    b:Rect = cell.bounds
                    b.merge(cell_rect)
                row.append(cell)
            rows.append(row)
        return rows


    def consolidate_cell_contents(self):
        for t in self.tables:         
            for row in t.cells:
                for cell in row:
                    blocks = self.join_line_blocks(cell.blocks)   
                    cell.blocks = blocks

            

    def get_image(self):
        return "".join(self.image)
    

    def get_doc_image(self):
        return "".join(self.doc_image)
    

def visualize_rect_lines (*args, title:str="Lines", halo:float=3, line_color='blue'):
    """ Visualizes lists of Rect instances as lines in a Tk window. 
    
    :param args: One or more List[Rect] that get drawn as lines with a halo
    :param title: The window title
    :param halo: distance of the halo surrounding the line
    :param line_color: color of the line (not the halo)
    """
    import tkinter

    def draw_halo(cv:tkinter.Canvas, r:Rect):
        h = r.extend(halo)
        cv.create_polygon(h.x0, h.y0, h.x1, h.y0, h.x1, h.y1, h.x0, h.y1, h.x0, h.y0, fill='', outline='green', width=0.5)

    # Create an instance of tkinter frame or window
    win=tkinter.Tk()
    # Set the size of the tkinter window
    win.geometry("700x800")
    win.title(title)
    # Create a canvas widget
    canvas=tkinter.Canvas(win, width=700, height=700)
    canvas.pack()

    def on_done():
        win.destroy()
    closer = tkinter.Button(win, text="Done", command = on_done)
    closer.pack()

    for arg in args:
        rects:List[Rect] = arg
        for r in rects:
            if r.width >= r.height:
                canvas.create_line(r.x0, r.cy, r.x1, r.cy, fill=line_color, width=2)
            else:
                canvas.create_line(r.cx, r.y0, r.cx, r.y1, fill=line_color, width=2)
            draw_halo(canvas, r)

    # Enter the main loop
    win.mainloop()


def visualize_rects (*args, title:str="Lines", inset:float=2, rect_color='blue'):
    import tkinter

    def draw_halo(cv:tkinter.Canvas, r:Rect):
        h = r.inset(inset)
        cv.create_polygon(h.x0, h.y0, h.x1, h.y0, h.x1, h.y1, h.x0, h.y1, h.x0, h.y0, fill='', outline=rect_color, width=1)

    # Create an instance of tkinter frame or window
    win=tkinter.Tk()
    # Set the size of the tkinter window
    win.geometry("700x800")
    win.title(title)
    # Create a canvas widget
    canvas=tkinter.Canvas(win, width=700, height=700)
    canvas.pack()

    def on_done():
        win.destroy()
    closer = tkinter.Button(win, text="Done", command = on_done)
    closer.pack()

    for arg in args:
        rects:List[Rect] = arg
        for r in rects:
            draw_halo(canvas, r)

    # Enter the main loop
    win.mainloop()


def visualize_rects_and_blocks (rects:List[Rect], blocks:List[Rect], title:str="Lines", inset:float=2, rect_color='blue', block_color='#808080', block_outline=''):
    import tkinter

    def draw_halo(cv:tkinter.Canvas, r:Rect):
        h = r.inset(inset)
        cv.create_polygon(h.x0, h.y0, h.x1, h.y0, h.x1, h.y1, h.x0, h.y1, h.x0, h.y0, fill='', outline=rect_color, width=1)

    # Create an instance of tkinter frame or window
    win=tkinter.Tk()
    # Set the size of the tkinter window
    win.geometry("700x800")
    win.title(title)
    # Create a canvas widget
    canvas=tkinter.Canvas(win, width=700, height=700)
    canvas.pack()

    def on_done():
        win.destroy()
    closer = tkinter.Button(win, text="Done", command = on_done)
    closer.pack()

    for r in rects:
        draw_halo(canvas, r)

    for r in blocks:
        canvas.create_rectangle(r.x0, r.y0, r.x1, r.y1, fill=block_color, outline=block_outline)

    # Enter the main loop
    win.mainloop()