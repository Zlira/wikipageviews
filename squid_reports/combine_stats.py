from datetime import date
from os import path

import pandas as pd
import ggplot as gp

data_path = path.join(
    path.dirname(path.realpath(__file__)), 'data'
)


def date_from_quarter(quarter):
    year, q = quarter.split()
    month = int(q[1]) * 3
    return pd.Timestamp(date(year=int(year), month=month, day=1))


def process_older_stats():
    stats = pd.read_csv(
        path.join(data_path, 'older_stats.csv'), sep='\t',
    )
    cols_to_return = ['Date', 'Ukrainian', 'Russian', 'English', 'Other']

    # convert quarters into dates
    stats['Date'] = stats['Quarter'].apply(date_from_quarter)
    stats = stats.drop(['Quarter', 'Share'], axis=1)

    # strip % from values
    percent_colnames = list(
        filter(lambda x: x != 'Date', stats.columns)
    )
    stats[percent_colnames] = stats[percent_colnames].apply(
        lambda x: x.str.strip('%').astype(float)
    )

    # sum all columns that are not of interest into 'Other'
    other_colnames = list(
        filter(lambda x: x not in cols_to_return, stats.columns)
    )
    stats['Other'] = stats[other_colnames].sum(axis=1)
    return stats[cols_to_return]


def process_newer_stats():
    stats = pd.read_csv(
        path.join(data_path, 'newer_stats.csv'), sep='\t',
        infer_datetime_format=True, parse_dates=['Date']
    )
    stats = stats.drop(['Portal'], axis=1)
    return stats


def load_combined_df():
    return pd.concat(
        [process_older_stats(), process_newer_stats()], ignore_index=True
    )


def plot(df):
    g = gp.ggplot(df, gp.aes(x='Date', y='value', color='variable')) + \
        gp.geom_line()
    print(g)

if __name__ == '__main__':
    df = load_combined_df()
    plot(df)
