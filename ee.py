# This is a script, so it needs a script docstring.
"""Usage: see the argparse module in python standard library."""

import concurrent.futures
import configparser
import functools
import io
import itertools
import logging
import os.path
import re
import sys
import time
import zipfile

import pandas as pd
import tika.parser
from fuzzywuzzy import fuzz, process
from xlsxwriter.utility import xl_rowcol_to_cell

pool = concurrent.futures.ProcessPoolExecutor()
config = configparser.ConfigParser()
config.read('config.ini')
logging.basicConfig(filename='copa.log', filemode='w', level=logging.INFO)

# <assume> Target text doesn't exceed page 9 and at least three footnote.
ft_pat3 = re.compile(
        r"""(?P<f1>(\n){4}([^\n]+\n\n[^\n]*){,4})  # Before page number
        0?[1-9]  # Page number variable
        (?P<f2>([^\n]+\n\n(?!\n)){,4})  # After page number
        .+?(?P=f1)0?[1-9](?P=f2).+?(?P=f1)0?[1-9](?P=f2)""", re.S | re.X)


# Returns text without footnote except at the beginning.
def parse(buffer):
    parsed = tika.parser.from_buffer(buffer)
    text = parsed['content'].lstrip('\n')
    # Keeping first 9 pages.
    cut = text.find('\n\n\n ')
    text = text[cut :]
    for i in range(9):
        cut = text.find('\n\n\n ', cut + 4)
    text = text[: cut]
    # Removes footnotes.
    m = ft_pat3.search(text)
    if m:
        ft_pat = re.compile(
                repr(m.group('f1'))[1 : -1]  # deal with backslash
                + r'0?[1-9]' + repr(m.group('f2'))[1 : -1])
        footnote = ft_pat.pattern.lower().replace(' policy', '\npolicy')
        text = footnote + ft_pat.sub('', text)
    return text


# Yields name and other name from symonym dicts.
def get_alias(name, junk=config['junk']):
    if '(' in name:
        index = name.find('(')  # Deal with parenthesis
        name = name.replace(')', '')  # Remove ')'
        appendage = name[index+1 :]
        if appendage in junk:
            name = name[: index]
        else:
            name = appendage
    name = name.strip().lower()
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


class Task(concurrent.futures.Future):
    """A wrapper of cpu bound generator for future."""

    def __init__(self, gen):
        super().__init__()
        self._gen = gen

    def step(self, value=None):
        try:
            f = self._gen.send(value)
            f.add_done_callback(self._wakeup)
        except StopIteration as exc:
            self.set_result(exc.value)

    def _wakeup(self, f):
        result = f.result()
        self.step(result)


class Excel(object):
    """Produce some excel files based on a template."""

    with pd.ExcelFile('template.xlsx') as xls:
        tdf = pd.read_excel(xls, 'template', index_col=0)
        xls.sheet_names.remove('template')
        extra_data = pd.read_excel(xls, xls.sheet_names)
        vdf = extra_data.get('valid')

    exo = functools.partial(process.extractOne, scorer=fuzz.ratio)

    def __init__(self, buffer):
        self.text = parse(buffer)
        self.data = self.tdf['value':'value'].applymap(self.mystrip)

    # Extracts excel formula by dataframe.
    @staticmethod
    def mystrip(value):
        try:
            return value.strip('[]')
        except AttributeError:
            return value

    def transform_currency(self, value):
        value = config.get('currency', value, fallback=value)
        return value.replace('\n', '')

    def search(self, pattern, groups, columns):
        m = re.search(pattern, self.text)
        if m:
            origin_value = self.transform_currency(m.group(groups[0]))
            origin_column = columns[0]
            for g, c in itertools.zip_longest(groups, columns):
                try:
                    value = self.transform_currency(m.group(g))
                    self.data.at['value', c] = value
                except IndexError:
                    self.derive(origin_column, origin_value, c)
            data = self.data.loc[:, self.data.columns.isin(columns)]
            return data

    def search_column(self, column, patinfo):
        pattern_name, groupinfo, relation = patinfo.split(':')
        groups = groupinfo.split(',')
        columns = (column + ',' + relation).rstrip(',').split(',')
        hp = config['pattern'][pattern_name]
        for alias in get_alias(column):
            wp = hp.replace('{h}', r'\s*'.join(alias.split()), 1)
            result = yield pool.submit(self.search, wp, groups, columns)
            if result is not None:
                return result
        else:
            logging.warning('Value of %s not found.' % (column))

    # Gets relation value in df by column and assigns to self.data.
    def derive(self, origin_column, origin_value, target_column):
        df = self.extra_data.get(target_column, self.vdf)
        for header in df.columns:
            if header in origin_column or origin_column in header:
                choice = df[header].dropna()
                break
        alternative = self.exo(origin_value, choice)[0]
        if target_column == origin_column:  # Converts value
            self.data.at['value', origin_column] = alternative
        else:  # Gets matching value
            subdf = df[df[header] == alternative]
            self.data.at['value', target_column] = subdf.iloc[0][target_column]

    def export(self):
        """Creats an excel in a temporary directory using self.data."""
        s_re = self.tdf.loc['re'].dropna()
        tasks = []
        for index, value in s_re.items():
            t = Task(self.search_column(index, value))
            t.step()
            tasks.append(t)
        for t in concurrent.futures.as_completed(tasks):
            self.data.update(t.result())
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine='xlsxwriter') as writer:
            workbook = writer.book
            num_fmt = workbook.add_format({'num_format': '#,##0.00'})
            pct_fmt = workbook.add_format({'num_format': '0%'})
            self.data.to_excel(writer, index=False)
            dfsheet = writer.sheets['Sheet1']
            if self.vdf is not None:
                self.vdf.to_excel(writer, 'DV', index=False)
                dvsheet = writer.sheets['DV']
            for number, name in enumerate(self.tdf):
                width = self.tdf.at['width', name]
                fmt = locals().get(self.tdf.at['format', name])
                cell = xl_rowcol_to_cell(1, number)
                # Cell format.
                dfsheet.set_column('{0}:{0}'.format(cell[0]), width, fmt)
                # Data validation.
                dv_box = self.tdf.at['dropdownlist', name]
                if pd.notna(dv_box):
                    dvsheet.set_column('{0}:{0}'.format(dv_box[0]), None, fmt)
                    dfsheet.data_validation(
                            cell,
                            {'validate': 'list', 'source': "='DV'!" + dv_box})
        bio.seek(0)
        return bio.read()


def main():
    zip = os.path.basename(sys.argv[1])
    with zipfile.ZipFile(zip, mode='a') as myzip:
        pdf_names = (x for x in myzip.namelist() if x.endswith('pdf'))
        for pdf in pdf_names:
            xls_name = pdf.replace('.pdf', '.xlsx')
            logging.info('[%s]' % (xls_name))
            xls = Excel(myzip.read(pdf))
            workbook = xls.export()
            myzip.writestr(xls_name, workbook)


if __name__ == '__main__':
    main()
