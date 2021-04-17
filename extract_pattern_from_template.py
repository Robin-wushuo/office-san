import configparser
import itertools
import pandas as pd
import re


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
            synonyms.append([k] + [x.strip() for x in v.split(',')])
            name = name.replace(k, '')
    synonyms.extend([x] for x in name.split())
    synonyms.sort(key=lambda x: origin_name.index(x[0]))
    possible_names = itertools.product(*synonyms)
    for tuple_form_name in possible_names:
        yield ' '.join(tuple_form_name).strip()


def get_column_name(name):
    mo = re.search(r'(\d+)\w?$', name)
    if mo:
        number = int(mo.group(1))
        name = template.columns[number]
        return name.lower()
    else:
        number = tuple(template.columns).index(name)
        return number


def produce_keyword_pattern(name):
    # Use free variable 'for_search'.
    general_pattern = '(?P<{id}>{content})'
    column_number = get_column_name(name)
    id = 'column{}'.format(column_number)
    patterns = (x.replace(' ', r'[ \n]+') for x in get_alias(name))
    content = '|'.join(patterns)
    keyword_pattern = general_pattern.format(id=id, content=content)
    return keyword_pattern


def main():
    config = configparser.ConfigParser()
    config.read('config.ini')
    config['keyword_pattern'] = {}
    for_search = template.loc['search', template.loc['search'].notna()]
    col_serie = for_search.index.to_series()
    keyword_pattern = col_serie.apply(produce_keyword_pattern)
    for name, pattern in keyword_pattern.items():
        config['keyword_pattern'][name] = pattern
    with open('config.ini', mode='w') as configfile:
        config.write(configfile)


if __name__ == '__main__':
    main()
