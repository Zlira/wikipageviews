import argparse
from contextlib import contextmanager
import logging

from lxml import etree
from sqlalchemy.orm.exc import NoResultFound

from db.db_conf import Session
from db.db_conf import TestSession
from db.wiki_tables import Page
from db.wiki_tables import Revision
from db.wiki_tables import Text


# TODO deal with these sessions!
session = Session()
LOGGER = logging.getLogger('xml-parser')
LOGGER.addHandler(logging.FileHandler(
    '/home/zlira/wiki_pageviews/logs/xml_parser.log'
))
LOGGER.addHandler(logging.StreamHandler())


def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n", "--page-number", type=int, help="Number of pages to parse"
    )
    parser.add_argument(
        "-t", "--test", action="store_true", help="Use test database"
    )
    return parser

# helpers


@contextmanager
def fading_element(element):
    """Context manager used to clear xml element after
    prcessing it."""
    yield element
    element.clear()


def tag_wo_ns(element):
    """Strips the namespace from tag name if it's present"""
    return element.tag.split('}')[-1]


def find_in_default_ns(element, tag):
    """Modification to 'find' method that allows to find
    element with tag name 'tag' in main namespace that
    doesn't have a prefix"""
    nsmap = {'defualt': element.nsmap[None]}
    return element.find('defualt:' + tag, namespaces=nsmap)


# TODO move helpers related to db into db package

def get_table_colums(obj):
    """For an slqalchemy model or instance of a model (obj)
    returns a names of colums of a respective table in the db.
    They may be different for model attributes that correspond
    to those columns (but currently aren't)."""
    return set(obj.__table__.columns.keys())


class DumpXmlParser:
    def __init__(self, file_path, session_maker):
        self.file_path = file_path
        self.curr_page = None
        self.curr_revision = None
        self.text_id_counter = 0
        self.session = session_maker()

    def parse(self, page_limit=None):
        page_counter = 0
        with open(self.file_path, 'rb') as xml_dump:
            element_iter = etree.iterparse(xml_dump)
            for event, element in element_iter:
                if tag_wo_ns(element) == 'page':
                    try:
                        with fading_element(element) as page:
                            self.process_page(page)
                            self.session.commit()

                            # clean session to prevent memory leaks?
                            self.session.expunge_all()
                        if page_limit and page_counter >= page_limit:
                            break
                    except Exception:
                        page_title = find_in_default_ns(element, 'title')
                        LOGGER.exception('Error while parsing element: %s',
                                         page_title.text)
                    else:
                        # count olny if the page was added
                        page_counter += 1

    def process_element(self, element, obj, special_cases):
        for child in element.iterchildren():
            stripped_tag = tag_wo_ns(child)
            if stripped_tag in get_table_colums(obj):
                setattr(obj, stripped_tag, child.text)
            elif stripped_tag in special_cases:
                getattr(self, special_cases[stripped_tag])(child)
        return obj

    def process_page(self, page):
        self.curr_page = Page()
        self.process_element(page, self.curr_page, {
            'revision': 'process_revision',
        })

        self.session.add(self.curr_page)
        # TODO remove this
        print(self.curr_page)

    def process_revision(self, revision):
        self.curr_revision = Revision()
        self.process_element(revision, self.curr_revision, {
            'text': 'process_text',
            'contributor': 'process_contributor',
        })
        self.curr_page.revisions.append(self.curr_revision)
        print(self.curr_revision.timestamp)
        self.session.add(self.curr_revision)
        revision.clear()

    def process_text(self, text):
        text_size = 0 if text.text is None else len(text.text)
        text_obj = Text(text_size=text_size)
        self.curr_revision.text = text_obj

    def process_contributor(self, contributor):
        ip = find_in_default_ns(contributor, 'ip')
        id_ = find_in_default_ns(contributor, 'id')
        if ip is not None:
            self.curr_revision.user_ip = ip.text
        elif id_ is not None:
            self.curr_revision.user_id = id_.text


if __name__ == '__main__':
    file_path = (
        '/media/storage/zlira/wiki/dumps/'
        'ukwiki-20161001-pages-meta-history.xml'
    )
    arg_parser = get_arg_parser()
    args = arg_parser.parse_args()
    parser = DumpXmlParser(
        file_path, session_maker=Session if not args.test else TestSession
    )
    parser.parse(page_limit=args.page_number)
