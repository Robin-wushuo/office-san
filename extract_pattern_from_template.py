import configparser
import itertools
import pandas as pd


with pd.ExcelFile('template.xlsx') as xls:
    template = pd.read_excel(xls, 'template', index_col=0)

config = configparser.ConfigParser()
config.read('config.ini')


def get_alias(name):
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
    possible_names = itertools.product(*synonyms)
    for tuple_form_name in possible_names:
        yield ' '.join(tuple_form_name).strip()


def get_group(names):
    try:
        name_list = names.split(',')
    except AttributeError:
        return names
    else:
        # x must be in config.ini .
        group_iterator = map(lambda x: config['group_user'][x], name_list)
        group = ','.join(group_iterator)
        return group


def collect_from_template():
    for_search = template.loc[
            ['search', 'group_user'], template.loc['search'].notna()]
    # Add new rows.
    group_user = for_search.loc['group_user']
    group = group_user.apply(get_group)
    for_search.loc['group'] = group
    for_search.loc['value'] = ''
    return for_search


def main():
    for_search = collect_from_template()

    def get_column_name_pattern(name):
        # Use free variable 'for_search'.
        general_pattern = '(?P<{id}>{content})'
        column_number = tuple(for_search.columns).index(name)
        id = 'column{}'.format(column_number)
        patterns = (x.replace(' ', r'[ \n]+') for x in get_alias(name))
        content = '|'.join(patterns)
        column_name_pattern = general_pattern.format(id=id, content=content)
        return column_name_pattern

    col_serie = for_search.columns.to_series()
    column_name_pattern = col_serie.apply(get_column_name_pattern)
    config = configparser.ConfigParser()
    config.read('config.ini')
    config['column_name_pattern'] = {}
    for column_name, pattern in column_name_pattern.items():
        config['column_name_pattern'][column_name] = pattern
    with open('config.ini', mode='w') as configfile:
        config.write(configfile)
    with pd.ExcelWriter('re_patterns.xlsx') as writer:
        for_search.to_excel(writer, sheet_name='for_search')
        # Is this needed?
        # template.to_excel(writer, sheet_name='Sheet2')

if __name__ == '__main__':
    main()
