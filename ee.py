# This is a script, so it needs a script docstring.
"""usage: see the argparse module in python standard library."""

import functools
import itertools
import logging
import os
import os.path
import re
import reprlib

import pandas as pd
import tika.parser

from argparse import ArgumentParser

from fuzzywuzzy import fuzz, process
from xlsxwriter.utility import xl_rowcol_to_cell
from tika import detector


# TODO(Robin) Store synonym_dict in a configure xml file.

logging.basicConfig(filename='copa.log', filemode='w', level=logging.INFO)
exo = functools.partial(process.extractOne, scorer=fuzz.ratio)
junk = {'from', 'to', 'full name'}
general_pat = r'(?i:\n{h})\s*(\s+\w+\s+)??\s*[:：]?\s*(?P<value>{v})'
risk_pat = r'(?ia:\b[a-z ()]+(?=insurance)|[ ]+\w[\w ]+)'
policy_pat = r'(?a:\w+)'

currency_pat = (
        r"""(?x:
            (?sia:.?\w(.{,400}[\n ]total([ ]\w+){,2}[: ]?
                |.{,50}attachment.{,50}))??
            (?P<p1>([EURSDACNYHKJPMBGZ]{3}|[€$¥£HK]{1,3}))\s*?
            (?P<p2>\d{1,3}(?:,?\d{3})*(?:\.\d+)?))""")

date_pat = (  # 1 January 2019 | 2019-1-30
        r"""(?x:(?i:from[ ])?
            (?P<p1>(\d?\d:\d\d[ ])?\w+[.\/, -]\w+[.\/, -]{1,2}(20)?\d{1,2})
            [ TOto,~\/>:-]+
            (?P<p2>(\d?\d:\d\d[ ])?\w+[.\/, -]\w+[.\/, -]{1,2}(20)?\d{1,2}))"""
        )

company_pat = (
        r"""(?ix:\b[a-z&() ,]+
            \s*?([a-z&() ,]+(?=(?P<Co>((Co[., ]+)?Ltd\.?|limited|company)))
                (?P=Co))(?P<comma>,)?(?(comma)[a-z&() ,]+)
            (\s*?[a-z (),]+(?=center)center)?
            (\s*?(\w+[ ]){,3}(?=(?P<branch>(sub)?[ -]?(branch|br|b\b|bch|sub)))
                (?P=branch))?)""")

# <assume> Target text doesn't exceed page 9 and at least three footnote.
ft_pat3 = re.compile(
        r"""(?P<f1>(\n){4}([^\n]+\n\n[^\n]*){,4})  # Before page number
        0?[1-9]  # Page number variable
        (?P<f2>([^\n]+\n\n(?!\n)){,4})  # After page number
        .+?(?P=f1)0?[1-9](?P=f2).+?(?P=f1)0?[1-9](?P=f2)""", re.S | re.X)

name_synonym = {
        'Premium Total Amount': ['Total Premium','Gross Annual Premium',
            'ANNUAL MINIMUM& DEPOSIT PREMIUM'],
        'Sum Insured': ['Limit of liability', 'Insured Interest'],
        'Policy Period': ['Period'],
        }

word_synonym = {
        'Client': ['Insured', 'Assured'],
        'Name': ['Entity', ''],
        'Risk': ['Type', 'Insurance Class', 'Class'],
        'Policy': ['Insured'],
        'No.': ['Number'],
        'Sum': ['Total'],
        }


def tikaparse(file):
    # Returns text of file without footnote except at the beginning.
    parsed = tika.parser.from_file(file)
    text = parsed['content']
    text = text[: len(text)//2]  # Arbitrary
    m = ft_pat3.search(text)
    if m:
        # Uses repr() to match a backslash.
        ft_pat = re.compile(
                r'\n(\ndict://key[.][\dA-Z]+/[\da-z%]+)?'
                + repr(m.group('f1'))[3 : -1]
                + r'0?[1-9]' + repr(m.group('f2'))[1 : -1])
        ft = ft_pat.pattern.lower().replace(' policy', '\npolicy')
        text = ft + ft_pat.sub('', text)
    return text


def _fill_g(name, pattern, junk=junk):
    # Fills general_pat with names and pattern and yields the pattern.
    pat_v = general_pat.replace('{v}', pattern, 1)
    index = name.find("(")  # Select "(...)" or not
    if index != -1:
        name = name.replace(")", '')
        appendage = name[index+1 :]
        if appendage.lower() in junk:
            name = name[: index]
        else:
            name = appendage
    for alias in get_alias(name):  # Maybe some other names
        yield pat_v.replace('{h}', alias.replace(' ', r'\s+'), 1)


def get_match(text, name, pattern):
    pats = _fill_g(name, pattern)  # pattern generator
    for pat in pats:
        m = re.search(pat, text)
        if m:
            return m


def _join_name(list_list):
    # Yields all possible names from a list of synonyms.
    possible_names = itertools.product(*list_list)  # Tuple form name
    for tuple_form_name in possible_names:
        yield ' '.join(tuple_form_name).strip()


def get_alias(name):
    # Yields name and other name from symonym dicts.
    if name in name_synonym:
        yield from (x for x in name_synonym[name])
    origin_name, synonyms = name, []
    for k, v in word_synonym.items():
        if k in name:
            synonyms.append([k] + v)
            name = name.replace(k, '')
    remainder = [[x] for x in name.split()]
    synonyms = synonyms + remainder
    synonyms.sort(key=lambda x: origin_name.index(x[0]))
    yield from _join_name(synonyms)


# Can Excel use other template?
# pandas read all worksheets?
# Puts re pattern into xml file?
class Excel(object):
    """Produce some excel files based on a template.

    In order to meet the team requirement, template excel contanining
    some informations about dropdown list, formula, width, format, re
    and derive."""

    with pd.ExcelFile('background/template.xlsx') as xls:
        tdf = pd.read_excel(xls, 'template', index_col=0)
        xls.sheet_names.remove('template')
        extra_data = pd.read_excel(xls, xls.sheet_names)
        vdf = extra_data.get('valid')

    formula = tdf.loc['formula':'formula']
    formula = formula.apply(lambda x: x.str.strip('[]'), axis=1)
    formula.rename({'formula': 'value'}, axis='index', inplace=True)
    glb = globals()

    # Contains self.find_in_text and more methods.
    def __init__(self, filename):
        self.filename = filename
        self.data = self.tdf['value':'value'].combine_first(self.formula)

    def find_in_text(self, text):
        """Extracts value from text and assigns to self.data."""
        logging.info('file [%s]' % (self.filename))
        gmt = functools.partial(get_match, text)
        it = self._write_column()
        next(it)
        s_re = self.tdf.loc['re'].dropna()
        for name, pat_gp in s_re.items():
            pat, gp = pat_gp.split()
            if pat != 'last':
                m = gmt(name, self.glb[pat])
            if m:
                it.send((name, m.group(gp)))
                logging.info('%s - %s' % (name, reprlib.repr(m.group(gp))))
            else:
                logging.warning('%s not found' % (name))
        logging.info('%s\n' % ('-'*40))
        it.close()

    def _write_column(self):
        while 1:
            column, value = yield  # Push
            self.data.at['value', column] = value.replace('\n', '')

    def derive(self, name1, name2):
        # Gets name2 value in df by name1 and assigns to self.data.
        df = self.extra_data.get(name2, self.vdf)
        column1 = exo(name1, df.columns)[0]
        value = exo(self.data.at['value', name1], df[column1])[0]
        if name1 == name2:
            self.data.at['value', name1] = value
        else:  # df must has two columns to get column2.
            subdf = df[df[column1] == value]
            column2 = df.drop([column1], axis=1).columns[0]
            self.data.at['value', name2] = subdf.iloc[0][column2]

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
                # How about format?
                dvsheet.set_column('A:A', None, pct_fmt)
            for number, name in enumerate(self.tdf):
                width = self.tdf.at['width', name]
                fmt = locals().get(self.tdf.at['format', name])
                cell = xl_rowcol_to_cell(1, number)
                # Cell format.
                dfsheet.set_column('{0}:{0}'.format(cell[0]), width, fmt)
                # Data validation.
                dv_box = self.tdf.at['dropdownlist', name]
                if pd.notna(dv_box):
                    dfsheet.data_validation(
                            cell,
                            {'validate': 'list', 'source': "='DV'!" + dv_box})


def main():
    # Creats a excel file from a pdf file and adds the excel to zipfile.
    parser = ArgumentParser(
            description="Copy the content from one to the other.")
    parser.add_argument(
            'input_dir', help="A directory containing the files to be read.")
    parser.add_argument('zipfile', help="Name of the output file.")
    args = parser.parse_args()
    output = zipfile.ZipFile(args.zipfile, mode='a')
    for root, dirs, files in os.walk(args.input_dir):
        fs = (f for f in files if detector.from_file(f).endswith('pdf'))
        for file in fs:
            filename = os.path.join(root, file)
            text = tikaparse(filename)
            xls = Excel(filename)
            xls.find_in_text(text)
            s_derive = xls.tdf.loc['derive'].dropna()
            for index, value in s_derive.items():
                xls.derive(value, index)
            # Finds currency.
            if xls.data.at['value', 'Currency (Sum Insured)'] == 'CNY':
                xls.data.at['value', 'Currency (Sum Insured)'] = (
                        'RMB - Renminbi')
            xls.data.at['value', 'Currency (Billing)'] = (
                    xls.data.at['value', 'Currency (Sum Insured)'])
            xls.export()
            output.write(filename)
    output.close()

if __name__ == '__main__':
    main()


def test2(file):
    text = tikaparse(file)
    xls = Excel(file)
    xls.find_in_text(text)

    s_derive = xls.tdf.loc['derive'].dropna()
    for index, value in s_derive.items():
        xls.derive(value, index)

    # Finds currency.
    if xls.data.at['value', 'Currency (Sum Insured)'] == 'CNY':
        xls.data.at['value', 'Currency (Sum Insured)'] = 'RMB - Renminbi'
    xls.data.at['value', 'Currency (Billing)'] = (
            xls.data.at['value', 'Currency (Sum Insured)'])

    xls.export()

def test3(file):
    text = TikaText.custom_parse(file)
    xls = Excel()
    xls.find_in_text(text)
    algrithm = (exo,)
    name1 = xls.data.at['value', 'Client Name (Full Name)']
    name2 = xls.data.at['value', 'Insurer Name (Full Name)']
    name3 = xls.data.at['value', 'Risk']
    for number, func in enumerate(algrithm):
        print(number+1, ' : ', func)
        print('Name1 :', name1)
        print(func(name1, xls.clidf['Client Name']))
        print('Name2 :', name2)
        print(func(name2, xls.insdf['Insurer Name']))
        print('Name3 :', name3)
        print(func(name3, xls._vdf['Risk Name1']))
        print()
