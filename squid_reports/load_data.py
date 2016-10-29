from datetime import date
from collections import namedtuple
from os import path

from bs4 import BeautifulSoup
import pandas as pd
import requests

DATA_FILE_TEMPLATE = (
    'https://stats.wikimedia.org/archive/squid_reports/'
    '{year}-{month:02d}/SquidReportPageViewsPerCountryBreakdown.htm'
)
month_of_year = namedtuple('Month', ['year', 'month'])
file_dir = path.dirname(path.realpath(__file__))


def get_data_file(year, month):
    resp = requests.get(
        DATA_FILE_TEMPLATE.format(year=year, month=month)
    )
    resp.raise_for_status()
    return resp.text


def strip_percents(string):
    return float(string.strip('%'))


def parse_country_views(country_name, html):
    soup = BeautifulSoup(html, 'html.parser')
    lang_header = soup.find('a', id=country_name).parent.parent
    views = {}
    for sibling in lang_header.nextSiblingGenerator():
        header = sibling.find('th')
        if header == -1:
            continue
        elif 'lh3' in header.attrs['class']:
            return views
        else:
            value = strip_percents(sibling.find('td', class_='c').text)
            views[header.text] = value


def generate_date_range(start, end):
    for i in range(start.month - 1, (end.year - start.year) * 12 + end.month):
        month = i % 12 + 1
        year = start.year + int(i / 12)
        yield(date(year=year, month=month, day=1))


def load_data(country_name='Ukraine',
              start_date=month_of_year(2014, 12),
              end_date=month_of_year(2016, 8)):
    df = None
    for date_ in generate_date_range(start_date, end_date):
        html = get_data_file(date_.year, date_.month)
        views_dict = parse_country_views(country_name, html)
        views_dict['Date'] = date_
        if df is None:
            df = pd.DataFrame(columns=views_dict.keys())
        # fill missing values with Nones
        df.loc[len(df)] = [views_dict.get(col, 0) for col in df.columns]
    return df


if __name__ == '__main__':
    data = load_data()
    data.columns = [col.strip(' Wp') for col in data.columns]
    data.to_csv(path.join(file_dir, 'data', 'newer_stats.csv'),
                sep='\t', index=False)
    print(data)
