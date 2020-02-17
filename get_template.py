#!/usr/bin/env python3

import argparse
import pandas as pd

from xlsxwriter.utility import xl_cell_to_rowcol

source = (
        'A7:A15', 'A19:A21', 'A42:A44', 'B2:B11', 'C2:C13', 'D63:D84',
        'E2:E168')

def dfg(df, ranges):
    for dv_range in ranges:
        cells = dv_range.split(':')
        rowcol1 = xl_cell_to_rowcol(cells[0])
        rowcol2 = xl_cell_to_rowcol(cells[1])
        sub_df = df.iloc[rowcol1[0]-1:rowcol2[0], rowcol1[1]:rowcol2[1]+1]
        yield sub_df


def extract(df, ranges=source):
    sub_dfs = dfg(df, ranges)
    result = next(sub_dfs)
    for sub_df in sub_dfs:
        result = result.combine_first(sub_df)
    return result.reindex_like(df)


def add_column_to_insurer():
    """Adds a column containing no comma in name."""
    names = idf['Insurer Name']
    index = names.index
    l = []
    for name in names:
        name_list = name.replace(',', ' ').lower().split()
        name = ' '.join(name_list)
        l.append(name)
    series = pd.Series(l, index=index)
    idf['Insurer Name (drop comma)'] = series


def main():
    with pd.ExcelFile('background/Billing of Witzenmann.xlsx') as xls:
        tdf = pd.read_excel(
                xls, sheet_name='01 Multi-line policy Details')
        cdf = pd.read_excel(xls, sheet_name='client code')
        idf = pd.read_excel(xls, sheet_name='insurer code', header=[0, 1])
        vdf = pd.read_excel(xls, sheet_name='RiskName', usecols='A:D,H',
                nrows=167)
    tdf = tdf[0 : 1]
    tdf.columns = tdf.columns.str.strip()
    tdf.iloc[0] = '0_0'
    tdf.at[0, 'Premium VAT Rate'] = 0.06
    tdf.at[0, 'Commission Rate'] = 0.15
    tdf.at[0, 'Commission VAT Rate'] = 0.06
    tdf.at[0, 'Income Class'] = '9.Renewal'
    tdf.at[0, 'Program Type'] = '3.Locally Admitted Policies'
    tdf.at[0, 'Placement Executive'] = 'No PE involved'
    tdf.at[0, 'Distribution Channel'] = '1. Open Market(Non Facility)'
    cdf = cdf.dropna(how='all')
    pat = r'(?i)China|\bCo\b\.?|Company|Ltd\.?|Limited'
    cdf['Client Name'] = cdf['Client Name'].str.replace(pat, '###')
    idf = idf['Insurer Listing'][['Client Number', 'Insurer Name']]
    idf.rename({'Client Number': 'Insurer Code'}, axis=1, inplace=True)
    if args.add_insurer_column:
        add_column_to_insurer()
    vdf = extract(vdf)
    with pd.ExcelWriter('test/template.xlsx', engine='xlsxwriter') as writer:
        tdf.to_excel(writer, 'template', index=False)
        cdf.to_excel(writer, 'client_code', index=False)
        idf.to_excel(writer, 'insurer_code', index=False)
        vdf.to_excel(writer, 'data_validation', index=False)
        tdfsheet = writer.sheets['template']
        tdfsheet.set_column('B:B', 15)
        tdfsheet.set_column('C:D', 30)
        tdfsheet.set_column('E:E', 15)
        tdfsheet.set_column('F:F', 30)
        tdfsheet.set_column('G:Y', 25)


parser = argparse.ArgumentParser()
parser.add_argument(
        '-a', '--add_insurer_column', help="add column to insurer",
        action="store_true")
args = parser.parse_args()

if __name__ == '__main__':
    main()
