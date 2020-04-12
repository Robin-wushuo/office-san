# This is a script, so it needs a script docstring.
"""Usage: see the argparse module in python standard library."""

import configparser
import functools
import io
import itertools
import logging
import os.path
import re
import reprlib
import sys
import zipfile

import pandas as pd
import tika.parser
from fuzzywuzzy import fuzz, process
from xlsxwriter.utility import xl_rowcol_to_cell

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
def parse(buffer, target):
    parsed = tika.parser.from_buffer(buffer)
    text = parsed['content'].lstrip('\n')
    cut = text.find('\n\n\n ')
    text = text[cut :]
    for i in range(9):
        cut = text.find('\n\n\n ', cut + 4)
    text = text[: cut]
    m = ft_pat3.search(text)
    if m:
        ft_pat = re.compile(
                repr(m.group('f1'))[1 : -1]  # deal with backslash
                + r'0?[1-9]' + repr(m.group('f2'))[1 : -1])
        footnote = ft_pat.pattern.lower().replace(' policy', '\npolicy')
        text = footnote + ft_pat.sub('', text)
    target.send(text)
    target.close()


def coroutine(func):
    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        next(cr)
        return cr
    return start


@coroutine
def grep(pattern_dict, target):
    text = yield
    for k, v in pattern_dict.items():
        pattern_name, group, relation = v.split(':')
        if pattern_name != 'last':
            halfp = config['pattern'][pattern_name]
            for alias in get_alias(k):  # Includes all names
                wholep = halfp.replace('{h}', r'\s*'.join(alias.split()), 1)
                m = re.search(wholep, text)
                if m:
                    break
            else:
                logging.warning('%s not found' % (k))
                continue
        value = m.group(group)
        value = config.get('currency', value, fallback=value)
        logging.info('%s - %s' % (k, reprlib.repr(value)))
        target.send((k, value, relation))
    target.close()
    yield


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


class Excel(object):
    """Produce some excel files based on a template."""

    with pd.ExcelFile('template.xlsx') as xls:
        tdf = pd.read_excel(xls, 'template', index_col=0)
        xls.sheet_names.remove('template')
        extra_data = pd.read_excel(xls, xls.sheet_names)
        vdf = extra_data.get('valid')

    exo = functools.partial(process.extractOne, scorer=fuzz.ratio)

    def __init__(self, buffer):
        self.data = self.tdf['value':'value'].applymap(self.mystrip)
        s_re = self.tdf.loc['re'].dropna()
        parse(buffer, grep(s_re, self.derive()))

    @staticmethod
    def mystrip(value):
        try:
            return value.strip('[]')
        except AttributeError:
            return value

    # Gets relation value in df by column and assigns to self.data.
    @coroutine
    def derive(self):
        while True:
            column, col_value, relation = yield
            col_value = col_value.replace('\n', '')
            self.data.at['value', column] = col_value
            if relation:  # Derives from relationship
                df = self.extra_data.get(relation, self.vdf)
                for header in df.columns:
                    if header in column or column in header:
                        choice = df[header].dropna()
                        break
                rel_value = self.exo(col_value, choice)[0]
                if column == relation:  # Converts value of column
                    self.data.at['value', column] = rel_value
                else:  # Gets relation value
                    subdf = df[df[header] == rel_value]
                    self.data.at['value', relation] = subdf.iloc[0][relation]

    def export(self):
        """Creats an excel in a temporary directory using self.data."""
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
    zip = os.path.basename(sys.argv[1])  # Needs a .zip file in cwd
    with zipfile.ZipFile(zip, mode='a') as myzip:
        pdf_names = (x for x in myzip.namelist() if x.endswith('pdf'))
        for pdf_name in pdf_names:
            xls_name = pdf_name.replace('.pdf', '.xlsx')
            logging.info('[%s]' % (xls_name))
            xls = Excel(myzip.read(pdf_name))
            workbook = xls.export()
            myzip.writestr(xls_name, workbook)

if __name__ == '__main__':
    main()
