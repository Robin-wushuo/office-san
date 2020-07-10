#!/usr/bin/env python3

import pandas as pd
from xlsxwriter.utility import xl_cell_to_rowcol

template_name = 'background/Billing of Witzenmann.xlsx'

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


def main():
    with pd.ExcelFile(template_name) as xls:
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
    tdf.at[0, 'Premium (VAT Excluded)'] = '[=P2/(1+N2)]'
    tdf.at[0, 'Premium VAT TAX'] = '[=P2-M2]'
    tdf.at[0, 'Commission (VAT Excluded)'] = '[=U2/(1+S2)]'
    tdf.at[0, 'Commission VAT TAX'] = '[=R2*S2]'
    tdf.at[0, 'Commission Total Amount'] = '[=P2*Q2]'
    tdf.at[0, 'Income Class'] = '9.Renewal'
    tdf.at[0, 'Program Type'] = '3.Locally Admitted Policies'
    tdf.at[0, 'Placement Executive'] = 'No PE involved'
    tdf.at[0, 'Distribution Channel'] = '1. Open Market(Non Facility)'
    cdf = cdf.dropna(how='all')
    from configparser import ConfigParser
    config = ConfigParser()
    config.read('config.ini')
    # client_pat = r'(?i)China|\bCo\b\.?|Company|Ltd\.?|Limited'
    # cdf['Client Name'] = cdf['Client Name'].str.replace(client_pat, '###')
    idf = idf['Insurer Listing'][['Client Number', 'Insurer Name']]
    for key, pattern in config['standard'].items():
        idf['Insurer Name'] = idf['Insurer Name'].str.replace(pattern, ' ')
        cdf['Client Name'] = cdf['Client Name'].str.replace(pattern, ' ')
    cdf.rename({'Client Name': 'Client Name (Full Name)'}, axis=1, inplace=True)
    idf.rename({
        'Client Number': 'Insurer Code',
        'Insurer Name': 'Insurer Name (Full Name)'}, axis=1, inplace=True)
    vdf = extract(vdf)
    vdf.rename({'Risk Name1': 'Risk'}, axis=1, inplace=True)
    with pd.ExcelWriter('test/template.xlsx', engine='xlsxwriter') as writer:
        tdf.to_excel(writer, 'template', index=False)
        cdf.to_excel(writer, 'Client Code', index=False)
        idf.to_excel(writer, 'Insurer Code', index=False)
        vdf.to_excel(writer, 'valid', index=False)
        tdfsheet = writer.sheets['template']
        tdfsheet.set_column('B:B', 15)
        tdfsheet.set_column('C:D', 30)
        tdfsheet.set_column('E:E', 15)
        tdfsheet.set_column('F:F', 30)
        tdfsheet.set_column('G:Y', 25)


import pandas as pd
sheet_names = ['Client Code', 'Insurer Code', 'valid']


def get_dataframe(filename):
    with pd.ExcelFile(filename) as xls:
        for sheet_name in sheet_names:
            dataframe = pd.read_excel(xls, sheet_name=sheet_name)
            yield dataframe


def compare_template():
    old = get_dataframe('test/oldtemplate.xlsx')
    new = get_dataframe('test/template.xlsx')
    for old_data, new_data, name in zip(old, new, sheet_names):
        print('%s equal: %s' % (name, old_data.equals(new_data)))
