# This is a script, so it needs a script docstring.
"""Usage: see the argparse module in python standard library."""

import configparser
import concurrent.futures
import io
import itertools
import math
import os
import re
import tempfile
import zipfile

from collections import namedtuple, abc

import pandas as pd

from tika.parser import from_buffer as tika_parse
from fuzzywuzzy import fuzz
from fuzzywuzzy.process import extractOne
from xlsxwriter.utility import xl_rowcol_to_cell
from pytesseract import image_to_data, Output
from PIL import Image


config = configparser.ConfigParser()
config.read('config.ini')
current = None
ready_list = []
future_queue = {}
p_pool = concurrent.futures.ProcessPoolExecutor()
t_pool = concurrent.futures.ThreadPoolExecutor()
Match = namedtuple('Match', 'columns, groups, filename', defaults=[None])

with pd.ExcelFile('template.xlsx') as xls:
    template = pd.read_excel(xls, 'template', index_col=0)
    xls.sheet_names.remove('template')
    extra_data = pd.read_excel(xls, xls.sheet_names)
    data_validation = extra_data.get('valid')
    MATCHINFO = template.loc['Match_info'].dropna()


def schedule(g):
    ready_list.append(g)


def unschedule(g):
    ready_list.remove(g)


def loop():
    global current
    while ready_list:
        g = ready_list[0]
        current = g
        try:
            next(g)
        except StopIteration:
            unschedule(g)
        else:
            expire_timeslice(g)


def expire_timeslice(g):
    if g in ready_list:
        del ready_list[0]
        ready_list.append(g)


def block(queue):
    queue.append(current)
    unschedule(current)


def unblock(queue):
    if queue:
        g = queue.pop(0)
        schedule(g)


def get_future_queue(f):
    queue = future_queue.get(f)
    if not queue:
        queue = []
        future_queue[f] = queue
    return queue


def block_for_future(future):
    block(get_future_queue(future))


def wait(pool, function, *args, **kwargs):
    future = pool.submit(function, *args, **kwargs)
    block_for_future(future)
    yield
    return future.result()


def wait_for_event():
    if not future_queue:
        return False
    done, _ = concurrent.futures.wait(
            future_queue, return_when='FIRST_COMPLETED')
    for f in done:
        unblock(future_queue.pop(f))
    return True


def run():
    while True:
        loop()
        if not wait_for_event():
            return


def _cut(text, pages, page_break='\n\n\n '):
    # Make sure that len('_CUT') == len(page_break).
    _text = text.replace(page_break, '_CUT', pages+1)
    cut_point = _text.rfind('_CUT')
    return text[:cut_point]


# def _remove_footnote(text):
#     m = re.search(config['noheader_pattern']['text_with_footnote'], text)
#     if m:
#         footnote_pat = re.compile(
#                 repr(m.group('f1'))[1:-1]
#                 + r'\d?\d ' + repr(m.group('f2'))[1:-1])
#         # Keeps policy which is inside the footnote.
#         policy = footnote_pat.pattern.lower().replace(' policy', '\npolicy')
#         text = policy + footnote_pat.sub('', text)
#     return text

# Change wait function to decorator.
def pdfimage_string(filename):
    abs_path = os.path.abspath(filename)
    dirname = os.path.dirname(abs_path)
    with tempfile.TemporaryDirectory(dir=dirname) as image_dir:
        image_dir = os.path.join(image_dir, '')
        cmd = 'pdfimages -l 9 -tiff {} {}'.format(filename, image_dir)
        yield from wait(p_pool, os.system, cmd)
        with os.scandir(image_dir) as it:
            images = [x.path for x in it]
            images.sort()
        img = image_processing(images.pop(0))
        text = yield from image_string(img)
        for image_path in images:
            text += yield from image_string(image_path)
    return text


import noteshrink
from PIL import Image
import numpy as np
def image_processing(filename):
    parser = noteshrink.get_argument_parser()
    options = parser.parse_args([filename, '-s', '5', '-n', '2'])
    print('opened', filename)
    print('  saving {}...'.format(filename))
    img, dpi = noteshrink.load(filename)
    root, ext = os.path.splitext(filename)
    output_filename = '{}_processed{}'.format(root, ext)
    samples = noteshrink.sample_pixels(img, options)
    palette = noteshrink.get_palette(samples, options)
    quantized = noteshrink.quantize(img, bits_per_channel=3).astype(np.uint8)
    labels = noteshrink.apply_palette(quantized, palette, options)
    output_img = Image.fromarray(labels, 'P')
    output_img.putpalette(palette.flatten())
    print('  done\n')
    return output_img


def image_string(image):
    # TODO: tesseract table layout analysis.
    # TODO: OMP_THREAD_LIMIT=1
    # TODO: threshold image.
    # TODO: learn converting RGB to HSV@/copa/bg/tesseract/a 3D.
    # TODO: What is subprocess.call?
    df = yield from wait(
            p_pool, image_to_data, image, output_type=Output.DATAFRAME)

    # Sorts lines by 'top' and 'left'.
    columns = ['block_num', 'par_num', 'line_num']
    grouped = df.groupby(columns)
    line = grouped.head(1)
    line = line.query('line_num != 0 & width > 50 & height > 10')
    line = line.copy(deep=False)
    line.loc[:, 'top'] = line.top.apply(has_close_value(line.top))
    order_line = line.sort_values(['top', 'left'])

    # Reorder. block_num is prior to top in first column of table.
    page = df.query('level == 1')
    duplicates = order_line[order_line.duplicated('top', keep=False)]
    bins = list(page.top) + list(duplicates.top.unique()) + list(page.height)
    order_line.loc[:, 'group'] = pd.cut(order_line.top, bins=bins, right=False)
    reorder = pd.DataFrame()
    grouped = order_line.groupby('group')
    for name, group in grouped:
        duplicate = duplicates[duplicates.top == name.left]
        pair = group[group.block_num.isin(duplicate.block_num)]
        other = group[~group.block_num.isin(duplicate.block_num)]
        try:
            pair_group = pair.groupby('block_num')
        except KeyError:
            pass
        else:
            for _, grp in pair_group:
                reorder = reorder.append(grp)
        reorder = reorder.append(other)

    # Gets text from the ordered dataframe.
    order_line = reorder.loc[:, columns]
    order_df = order_line.merge(df).fillna('\n')
    text = ' '.join(order_df.text).replace('\n ', '\n')
    return text


# TODO: Learn: "Determining the number of clusters in a data set"@wiki
# TODO: Learn: "k-means clustering"@wiki
# TODO: Replaced by noteshrink.quantize
def has_close_value(num_list):
    groups = set()
    def _isclose(number):
        nonlocal num_list
        # Finds in exist groups.
        for min_value, max_value in groups:
            if (math.isclose(min_value, number, abs_tol=10)
                    or math.isclose(max_value, number, abs_tol=10)):
                return max_value
        # Finds in num_list.
        choices = list(num_list)
        findings = [number]
        for num in choices:
            if math.isclose(num, number, abs_tol=10):
                findings.append(num)
        boundary = (min(findings), max(findings))
        groups.add(boundary)
        num_list = filter(lambda x: x<boundary[0] or x>boundary[1], choices)
        return boundary[1]
    return _isclose


def alias(name):
    name_parts = name.partition('(')  # )
    name = name_parts[0].strip().lower()
    if name in config['name_synonym']:
        yield from (x.strip() for x in config['name_synonym'][name].split(','))
    origin_name, synonyms = name, []
    for k, v in config['word_synonym'].items():
        if k in name:
            synonyms.append([k] + v.split(','))
            name = name.replace(k, '')
    synonyms.extend([x] for x in name.split())
    synonyms.sort(key=lambda x: origin_name.index(x[0]))
    possible_names = itertools.product(*synonyms)  # Tuple form name
    for tuple_form_name in possible_names:
        yield ' '.join(tuple_form_name).strip()


# def collect_alias(pattern_name, column):
#     general = config['pattern'][pattern_name]
#     headers = []
#     for h in alias(column):
#         h = r'\s*'.join(h.split())
#         headers.append(h)
#     head_pattern = '(%s)' % '|'.join(headers)
#     pattern = general.replace('{h}', head_pattern, 1)
#     return pattern

def collect_alias(column_name):
    headers = [x.replace(' ', '[ \n]+') for x in alias(column_name)]
    head_pattern = '|'.join(headers)
    return head_pattern


def get_subgroup_name(name):
    try:
        result = config['group_user'][name]
    except AttributeError:
        result = name
    return result


def _get_function(value):
    try:
        return value.strip('[]')
    except AttributeError:
        return value


def fetch(info, text, dataframe, header=True):
    if header:
        pattern = collect_alias(info.pattern_name, info.column)
    else:
        try:
            pattern = config['noheader_pattern'][info.pattern_name]
        except KeyError:
            return dataframe
    # Find value from text by re.
    m = re.search(pattern, text)
    if m:
        for g, c in itertools.zip_longest(info.groups, info.columns):
            try:
                value = m.group(g)
            except IndexError:
                # Find value from dataframe by fuzzywuzzy.
                source = extra_data.get(c, data_validation)
                choices = source[info.column]
                to_match = dataframe.at['value', info.column]
                best_match = standard_match(to_match, choices.dropna())
                matched, ratio, _ = best_match
                if ratio < 80:
                    print('Match(%s)\nResult(%s)' % (to_match, matched))
                    print('Location(%s @%s)\n' % (info.column, info.filename))
                if c == info.column:
                    dataframe.at['value', c] = matched
                else:
                    selection = source[choices == matched]
                    dataframe.at['value', c] = selection.iloc[0][c]
            else:
                dataframe.at['value', c] = adjust(value)
    elif header:
        fetch(info, text, dataframe, header=False)
    return dataframe


def adjust(value):
    value = config.get('currency', value, fallback=value)
    value = value.replace('\n', '')
    return value


def standard(value):
    for key, pattern in config['standard'].items():
        value = re.sub(pattern, ' ', value)
    return value


def standard_match(value, choices):
    match = extractOne(standard(value), choices, scorer=fuzz.ratio)
    return match


def to_formatted_excel(bio, dataframe):
    with pd.ExcelWriter(bio, engine='xlsxwriter') as writer:
        # Write worksheet1 and worksheet2.
        dataframe.to_excel(writer, index=False)
        worksheet1 = writer.sheets['Sheet1']
        if data_validation is not None:
            data_validation.to_excel(writer, 'DV', index=False)
            worksheet2 = writer.sheets['DV']
        # Format worksheet1, and add data validation by using worksheet2.
        workbook = writer.book
        num_fmt = workbook.add_format({'num_format': '#,##0.00'})
        pct_fmt = workbook.add_format({'num_format': '0%'})
        for n, column in enumerate(template):
            width = template.at['width', column]
            format = locals().get(template.at['format', column])
            location1 = xl_rowcol_to_cell(1, n)
            cells_in_sheet1 = '{0}:{0}'.format(location1[0])
            worksheet1.set_column(cells_in_sheet1, width, format)
            location2 = template.at['dropdownlist', column]
            if pd.notna(location2):
                cells_in_sheet2 = '{0}:{0}'.format(location2[0])
                worksheet2.set_column(cells_in_sheet2, None, format)
                worksheet1.data_validation(
                        location1,
                        {'validate': 'list', 'source': "='DV'!" + location2})


class Crawler(object):

    def __init__(self, zip):
        self.zip = zipfile.ZipFile(zip, mode='a')
        self.max_worker = 3
        self.pdf_names = []
        self.data = {}

    def crawl(self):
        for filename in self.zip.namelist():
            if filename.endswith('pdf'):
                self.pdf_names.append(filename)
        for _ in range(self.max_worker):
            schedule(self.work())
        run()
        for name, dataframe in self.data.items():
            bio = io.BytesIO()
            to_formatted_excel(bio, dataframe)
            bio.seek(0)
            with open(name.replace('.pdf', '.xlsx'), mode='wb') as file:
                file.write(bio.read())
            # self.zip.writestr(name.replace('.pdf', '.xlsx'), bio.read())
        self.zip.close()

    def work(self):
        while self.pdf_names:
            filename = self.pdf_names.pop()
            text = yield from self.parse(filename)
            if not text:
                continue
            dataframe = template['value':'value'].applymap(_get_function)
            self.data[filename] = dataframe
            for match in handle(MATCHINFO):
                match = match._replace(filename=filename)
                schedule(self.dataframe_update(dataframe, text, match))

    def dataframe_update(self, dataframe, text, info):
        df = yield from wait(p_pool, fetch, info, text, dataframe)
        if dataframe[info.column].equals(df[info.column]):
            print('Warning: %s: %s failed.' % (info.filename, info.column))
        df = df.loc[:, df.columns.isin(info.columns)]
        dataframe.update(df)

    def parse(self, filename, cut_page=True):
        byte_string = yield from self.read(filename)
        parsed = yield from wait(t_pool, tika_parse, byte_string)
        text = parsed['content']
        if text:
            text = text.lstrip('\n')
            if cut_page:
                text = _cut(text, pages=9)
        else:
            with tempfile.TemporaryDirectory(dir='.') as dirname:
                self.zip.extract(filename, dirname)
                path = os.path.join(dirname, filename)
                text = yield from pdfimage_string(path)

        # text = yield from wait(p_pool, _remove_footnote, text)
        return text

    def read(self, filename):
        with self.zip.open(filename) as pdf_file:
            data = yield from wait(t_pool, pdf_file.read)
        return data


def find(text: str, keys: abc.Collection, start=0, basket={}):
    result1 = find1(text, keys, start)
    start, collected = result1.popitem()
    basket.update(collected)
    remain_keys = config['keyword_pattern'].keys() - basket.keys()
    if collected and remain_keys:
        basket = find(text, remain_keys, start, basket)
    elif remain_keys:
        print('"""', 'Not found')
        print(remain_keys)
        print('"""')
        print()
    return basket


finding = template.loc['value'].map(_get_function)
from extract_pattern_from_template import get_column_name
# TODO: Let fetch work as before.
# TODO: Add a function to check match result.
# TODO: How about define all keywords, include the useless ones?
# To make tabulating possible in this way. And even don't
# need to match content by regular expression.
# TODO: Is it able to use several found keys to find the schedule table.
# For example, use normal distribution to check key's position.
def find1(text: str, keys: abc.Collection, start: int = 0) -> dict:
    pattern = r"\n[\d. ]*(?i:%s)\s*(\s+\w+\s+)??\s*[:：]?\s*%s"
    patterns = []
    for k in keys:
        keyword = config['keyword_pattern'][k]
        content = config['content_pattern'][k]
        combine = pattern % (keyword, content)
        patterns.append(combine)
    complete_pattern = '|'.join(patterns)
    mo = re.search(complete_pattern, text[start:])
    if mo:
        start += mo.end()
        result = {start: {}}
        for k, v in mo.groupdict().items():
            if v and k.startswith('value'):
                name = get_column_name(k)
                result[start][name] = (result[start].setdefault(name, '')
                        + ' ' + v).strip()
    else:
        result = {0: {}}
    return result


import tika.parser
import pprint
import sys
def test():
    dataframe = template['value':'value'].applymap(_get_function)
    parsed = tika.parser.from_file(sys.argv[1])
    text = parsed['content']
    text = _cut(text, pages=9)
    all_keys = config['keyword_pattern'].keys()
    result = find(text, all_keys)
    pprint.pprint(result)

if __name__ == '__main__':
    test()
