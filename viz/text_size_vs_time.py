import sys

import pandas as pd
# this is just for the beauty
import seaborn
import matplotlib.pyplot as plt
from toolz.functoolz import pipe

from db.db_conf import TestSession
from db.wiki_tables import Page


session = TestSession()


def get_page(page_name):
    # FIXME this filter is case insensitive
    # for some articles like 'Білі хорвати' it
    # results in an error
    return (session.query(Page)
            .filter(Page.title == page_name)
            .one())


def construct_df(page):
    return pd.DataFrame(
        [(r.timestamp, r.text_size) for r in page.revisions],
        columns=['timestamp', 'text_size']
    )


def plot_size_vs_time(df):
    ax = df.plot('timestamp', 'text_size', kind='line', c='gray')
    df.plot('timestamp', 'text_size', kind='line', style='.', c='red', ax=ax)
    return ax

if __name__ == '__main__':
    page_name = sys.argv[1]
    pipe(
        page_name,
        get_page,
        construct_df,
        plot_size_vs_time,
    )
    plt.show()
