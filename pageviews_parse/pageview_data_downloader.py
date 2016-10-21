from calendar import monthrange
import codecs
from collections import namedtuple
from datetime import datetime
from functools import partial
from itertools import groupby
import logging
from pprint import pprint
from operator import attrgetter
from operator import itemgetter
import re
from urllib import parse

from fn import F
from influxdb import InfluxDBClient

BASE_URL = 'https://dumps.wikimedia.org/other/pagecounts-raw/'
logger = logging.getLogger('wiki')
logger.setLevel(logging.INFO)
handler = logging.FileHandler('log.log')
logger.addHandler(handler)


# download files sequentially
# in each file select only uk.wiki articles from mail space (and main page?)
# aggregate views by day
# write to InfluxDB

# TODO maybe use annotations?


# Helpers


def generate_symbol_mapping(start, end):
    return {
        chr(i - start + ord('A')): i for i in
        range(start, end + 1)
    }


def apply_to_keys(dct, func):
    return {func(key): val for key, val in dct.items()}


def apply_to_vals(dct, func):
    return {key: func(val) for key, val in dct.items()}


class PagecountFileParser:
    line_parts = namedtuple('Line', [
        'project', 'title', 'agg_monthly_views', 'hourly_views'
    ])
    line_parts_separator = ' '
    pageviews_separator = ','
    days_mapping = generate_symbol_mapping(1, 31)
    hours_mapping = generate_symbol_mapping(0, 23)
    # TODO handle missing data
    hourly_views_pattern = re.compile(r'(?P<hour>[A-Z])(?P<views>\?|\d+)')
    missing_day = '*'
    missing_view = '?'

    def __init__(self, year, month, file_name, project):
        # TODO pass file name and infer year and month
        # from it
        self.year = year
        self.month = month
        self.file_name = file_name
        self.project = project

    def parse(self, aggregate_by_day=True):
        # count lines to make troubleshooting easier later
        line_counter = 0
        with codecs.open(
            self.file_name, 'r', encoding='utf-8', errors='ignore'
        ) as pagecount_file:
            lines = (line for line in pagecount_file
                     if line.startswith(self.project))
            for line in lines:
                try:
                    line_counter += 1
                    title, view_counts = self.parse_line(line)
                    print(title)
                    if aggregate_by_day:
                        view_counts = self.aggregate_days(view_counts)
                    yield title, apply_to_keys(
                        view_counts, lambda x: x.isoformat()
                    )
                except Exception:
                    logger.exception('Line number was {}'.format(line_counter))
                    continue

    def parse_visits(self, views_string):
        monthly_views = {}
        views = (view for view in
                 views_string.split(self.pageviews_separator)
                 if view)
        for day_views in views:
            day = self.days_mapping[day_views[0]]
            datetime_func = partial(
                datetime, year=self.year, month=self.month,
                day=day
            )
            hourly_views = day_views[1:]
            # data is missing for entire day
            if hourly_views.startswith(self.missing_day):
                hourly_views = {datetime_func(): None}
            else:
                hourly_views = self.process_hourly_views(
                    self.hourly_views_pattern.findall(hourly_views),
                    datetime_func,
                )
            monthly_views.update(hourly_views)
        return monthly_views

    def process_hourly_views(self, hourly_views, datetime_func):
        """
        hourly_views: iterable of two tuples (encoded_hour, view_count)
        datetime_func: datetime function with partially applied arguments
        such as year, month and day

        Returns dict with datetime objects as keys and view_count integers
        as values. If view_count is missing it is imputed as a mean of all
        other hours (taking into account that number of views for missing
        hours is 0). If there are only missing hours their view_counts are
        set to None.
        """
        hourly_views = apply_to_keys(
            dict(hourly_views), lambda x: datetime_func(hour=self.hours_mapping[x])
        )
        # No missing values
        if all(view != self.missing_view for view in hourly_views.values()):
            return apply_to_vals(hourly_views, int)
        # All missing values
        elif all(view == self.missing_view for view in hourly_views.values()):
            return apply_to_vals(hourly_views, lambda x: None)
        # Some missing values
        else:
            mean = round(sum(
                int(view) for view in hourly_views.values()
                if view != self.missing_view
            ) / 24)
            return apply_to_vals(
                hourly_views,
                lambda x: mean if x == self.missing_view else int(x)
            )

    def aggregate_days(self, monthly_view_count):
        # TODO refactor this - generalize impute function
        # for both hours and days

        # compose functions to get key's day
        day_getter = F(attrgetter('day')) << F(itemgetter(0))
        sorted_counts = sorted(
            monthly_view_count.items(), key=day_getter
        )
        aggregated = {}

        missing_values = False
        for day, group in groupby(sorted_counts, day_getter):
            view_counts = list(map(itemgetter(1), group))
            # at this point either all or none of the hourly
            # views should be None
            day = datetime(
                self.year, self.month, day
            )
            if all(count is None for count in view_counts):
                aggregated[day] = None
                missing_values = True
            else:
                aggregated[day] = sum(view_counts)
        if not missing_values:
            return aggregated
        # if there where missing values imputation is needed
        else:
            present_values = filter(lambda x: x is not None,
                                    aggregated.values())
            # if there are no present_values - just give up
            # TODO but log it somewhere
            if not present_values:
                return apply_to_vals(aggregated, lambda x: 0)
            days_in_month = monthrange(self.year, self.month)[1]
            mean = round(sum(present_values) / days_in_month)
            return apply_to_vals(
                aggregated, lambda x: mean if x is None else x
            )

    def parse_line(self, line):
        line = self.line_parts(*line.split())
        # titles are sometimes at least double quoted
        prev_title = ''
        title = line.title
        while title != prev_title:
            title, prev_title = parse.unquote_plus(title).strip(), title
        monthly_views = self.parse_visits(line.hourly_views)
        return title, monthly_views


def format_for_influx_query(title, time, value):
    return {
        'measurement': 'page_views',
        'tags': {'title': title},
        'time': time,
        'fields': {'value': value},
    }


def load_pageviews_to_influx(page_counts_data):
    client = InfluxDBClient('localhost', 8086, database='wiki_pageviews')
    for title, view_counts in page_counts_data:
        entries = [
            format_for_influx_query(title, time, value) for
            time, value in view_counts.items()
        ]
        client.write_points(entries)

if __name__ == '__main__':
    parser = PagecountFileParser(
        file_name='../data/pageviews/test_lines',
        year=2011, month=12, project='uk.z'
    )
    load_pageviews_to_influx(parser.parse())
