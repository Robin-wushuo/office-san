# This is a script, so it needs a script docstring.
"""Usage: see the argparse module in python standard library."""

from collections import namedtuple
import concurrent.futures
import configparser
import functools
import io
import itertools
import re
import zipfile

import pandas as pd
from tika.parser import from_buffer as tika_parse
from fuzzywuzzy import fuzz, process
from xlsxwriter.utility import xl_rowcol_to_cell

config = configparser.ConfigParser()
config.read('config.ini')
current = None
ready_list = []
future_queue = {}
p_pool = concurrent.futures.ProcessPoolExecutor()
t_pool = concurrent.futures.ThreadPoolExecutor()
Match = namedtuple('Match', 'column, pattern_name, groups, columns')
exo = functools.partial(process.extractOne, scorer=fuzz.ratio)

text_with_footnote_pat = re.compile(
        r"""(?P<f1>(\n){4}([^\n]+\n\n[^\n]*){,4})
        0?[1-9]  # Page number which is less than 9
        (?P<f2>([^\n]+\n\n(?!\n)){,4})
        .+?(?P=f1)0?[1-9](?P=f2).+?(?P=f1)0?[1-9](?P=f2)""", re.S | re.X)

with pd.ExcelFile('template.xlsx') as xls:
    template = pd.read_excel(xls, 'template', index_col=0)
    xls.sheet_names.remove('template')
    extra_data = pd.read_excel(xls, xls.sheet_names)
    data_validation = extra_data.get('valid')


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


def async_func(pool, function, *args):
    future = pool.submit(function, *args)
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


def _remove_footnote(text):
    m = text_with_footnote_pat.search(text)
    if m:
        footnote_pat = re.compile(
                repr(m.group('f1'))[1:-1]
                + r'0?[1-9]' + repr(m.group('f2'))[1:-1])
        # Keeps policy which is inside the footnote.
        policy = footnote_pat.pattern.lower().replace(' policy', '\npolicy')
        text = policy + footnote_pat.sub('', text)
    return text


def parse(pdf_data, cut_page=True):
    parsed = yield from async_func(t_pool, tika_parse, pdf_data)
    text = parsed['content'].lstrip('\n')
    if cut_page:
        text = _cut(text, pages=9)
    text = _remove_footnote(text)
    return text


def read(file):
    data = yield from async_func(t_pool, file.read)
    file.close()
    return data


def get_alias(name):
    # Produce alternative name.
    name_parts = name.partition('(')  # )
    name = name_parts[0].strip().lower()
    if name in config['name_synonym']:
        yield from (x for x in config['name_synonym'][name].split(','))
    origin_name, synonyms = name, []
    for k, v in config['word_synonym'].items():
        if k in name:
            synonyms.append([k] + v.split(','))
            name = name.replace(k, '')
    synonyms.extend([x] for x in name.split())
    synonyms.sort(key=lambda x: origin_name.index(x[0]))
    possible_names = itertools.product(*synonyms)  # Tuple form name
    for tuple_form_name in possible_names:
        yield ' '.join(tuple_form_name)


def get_pattern(name, column):
    general = config['pattern'][name]
    for h in get_alias(column):
        pattern = general.replace('{h}', r'\s*'.join(h.split()), 1)
        yield pattern


def handle(mapping):
    for k, v in mapping.items():
        pattern_name, groups, columns = v.split(':')
        groups = groups.split(',')
        columns = (k + ',' + columns).rstrip(',').split(',')
        yield Match(k, pattern_name, groups, columns)


def _get_function(value):
    try:
        return value.strip('[]')
    except AttributeError:
        return value


def fetch(info, text, dataframe):
    for p in get_pattern(info.pattern_name, info.column):
        # Find value from text by re.
        m = re.search(p, text)
        if m:
            for g, c in itertools.zip_longest(info.groups, info.columns):
                try:
                    value = m.group(g)
                except IndexError:
                    # Find value from dataframe by fuzzywuzzy.
                    source = extra_data.get(c, data_validation)
                    choice = source[info.column]
                    first_value = m.group(info.groups[0])
                    matched_value = exo(first_value, choice.dropna())[0]
                    if c == info.column:
                        dataframe.at['value', c] = matched_value
                    else:
                        selection = source[choice == matched_value]
                        dataframe.at['value', c] = selection.iloc[0][c]
                else:
                    dataframe.at['value', c] = standard(value)
            dataframe = dataframe.loc[:, dataframe.columns.isin(info.columns)]
            break
    return dataframe


def standard(value):
    value = config.get('currency', value, fallback=value)
    value = value.replace('\n', '')
    return value


def format_then_to_excel(bio, dataframe):
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
        self.pdf_files = []
        self.data = {}

    def crawl(self):
        for file in self.zip.namelist():
            if file.endswith('pdf'):
                pdf = self.zip.open(file)
                self.pdf_files.append(pdf)
        self.materials = template.loc['re'].dropna()
        for _ in range(self.max_worker):
            schedule(self.work())
        run()
        for name, dataframe in self.data.items():
            bio = io.BytesIO()
            format_then_to_excel(bio, dataframe)
            bio.seek(0)
            self.zip.writestr(name.replace('.pdf', '.xlsx'), bio.read())
        self.zip.close()

    def work(self):
        while self.pdf_files:
            pdf = self.pdf_files.pop(0)
            byte_string = yield from read(pdf)
            text = yield from parse(byte_string)
            dataframe = template['value':'value'].applymap(_get_function)
            self.data[pdf.name] = dataframe
            for match_info in handle(self.materials):
                schedule(self.update(dataframe, text, match_info, pdf.name))

    def update(self, dataframe, text, info, filename):
        df = yield from async_func(p_pool, fetch, info, text, dataframe)
        if df is dataframe:
            print('%s not found in %s.' % (info.column, filename))
        dataframe.update(df)
