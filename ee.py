# This is a script, so it needs a script docstring.
"""Usage: see the argparse module in python standard library."""

import configparser
import functools
import itertools
import logging
import os
import os.path
import re
import reprlib
import sys
import zipfile

import pandas as pd
import tika.parser
from fuzzywuzzy import fuzz, process
from xlsxwriter.utility import xl_rowcol_to_cell
from tika import detector

config = configparser.ConfigParser()
config.read('config.ini')
logging.basicConfig(filename='copa.log', filemode='w', level=logging.INFO)

# <assume> Target text doesn't exceed page 9 and at least three footnote.
ft_pat3 = re.compile(
        r"""(?P<f1>(\n){4}([^\n]+\n\n[^\n]*){,4})  # Before page number
        0?[1-9]  # Page number variable
        (?P<f2>([^\n]+\n\n(?!\n)){,4})  # After page number
        .+?(?P=f1)0?[1-9](?P=f2).+?(?P=f1)0?[1-9](?P=f2)""", re.S | re.X)


def tikaparse(file):
    # Returns text of file without footnote except at the beginning.
    parsed = tika.parser.from_file(file)
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
    return text


def get_match(text, name, pattern):
    pat_v = config['pattern'][pattern]
    for alias in get_alias(name):  # Includes all names
        pat = pat_v.replace('{h}', r'\s*'.join(alias.split()), 1)
        m = re.search(pat, text)
        if m:
            return m


def get_alias(name, junk=config['junk']):
    # Yields name and other name from symonym dicts.
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
    """Produce some excel files based on a template.

    In order to meet the team requirement, template excel contanining
    some informations about dropdown list, formula, width, format, re
    and derive."""

    with pd.ExcelFile('template.xlsx') as xls:
        tdf = pd.read_excel(xls, 'template', index_col=0)
        xls.sheet_names.remove('template')
        extra_data = pd.read_excel(xls, xls.sheet_names)
        vdf = extra_data.get('valid')

    exo = functools.partial(process.extractOne, scorer=fuzz.ratio)

    def __init__(self, filename, text):
        self.filename = filename
        self.data = self.tdf['value':'value'].applymap(self.mystrip)
        self.find_in_text(text)
        s_derive = self.tdf.loc['derive'].dropna()
        for index, value in s_derive.items():
            self.derive(value, index)

    @staticmethod
    def mystrip(value):
        try:
            return value.strip('[]')
        except AttributeError:
            return value

    def find_in_text(self, text):
        # Extracts value from text and assigns to self.data.
        logging.info('[%s]' % (self.filename))
        gmt = functools.partial(get_match, text)
        it = self._write_column()
        next(it)
        s_re = self.tdf.loc['re'].dropna()
        for name, pat_gp in s_re.items():
            pat, gp = pat_gp.split()
            if pat != 'last':
                m = gmt(name, pat)
            if m:
                value = m.group(gp)
                value = config.get('currency', value, fallback=value)
                it.send((name, value))
                logging.info('%s - %s' % (name, reprlib.repr(value)))
            else:
                logging.warning('%s not found' % (name))
        it.close()

    def _write_column(self):
        while 1:
            column, value = yield  # Push
            self.data.at['value', column] = value.replace('\n', '')

    def derive(self, name1, name2):
        # Gets name2 value in df by name1 and assigns to self.data.
        df = self.extra_data.get(name2, self.vdf)
        for header in df.columns:
            if header in name1 or name1 in header:
                choice = df[header].dropna()
                break
        original_value = self.data.at['value', name1]
        value = self.exo(original_value, choice)[0]
        if name1 == name2:  # Converts value of name1
            self.data.at['value', name1] = value
        else:  # Gets name2 value
            subdf = df[df[header] == value]
            self.data.at['value', name2] = subdf.iloc[0][name2]

    def export(self, filename=None):
        """Creats an excel in a temporary directory using self.data."""
        filename = filename or self.filename.replace('.pdf', '.xlsx', 1)
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
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
        return filename


def main():
    # Creats some excel files according to a folder of pdf files.
    # Shell copies zip to cwd such as tmp folder. Extratall files.
    zip = os.path.basename(sys.argv[1])
    with zipfile.ZipFile(zip, mode='a') as myzip:
        for root, dirs, files in os.walk(os.getcwd()):
            pdfs = (f for f in files if f.endswith('pdf'))
            for pdf in pdfs:
                filename = os.path.join(root, pdf)
                text = tikaparse(filename)
                xls = Excel(filename, text)
                xlsname = xls.export()
                myzip.write(xlsname, os.path.relpath(xlsname))

if __name__ == '__main__':
    main()
