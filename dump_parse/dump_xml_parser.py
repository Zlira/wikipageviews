import argparse
from collections import namedtuple
import logging

from lxml import etree

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
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
LOGGER.addHandler(stream_handler)
LOGGER.setLevel(logging.INFO)
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

tagged_event = namedtuple('TaggedEvent', ['event', 'tag'])


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


def tag_wo_ns(element):
    """Strips the namespace from tag name if it's present"""
    return element.tag.split('}')[-1]


def find_in_default_ns(element, tag):
    """Modification to 'find' method that allows to find
    element with tag name 'tag' in main namespace that
    doesn't have a prefix"""
    nsmap = {'defualt': element.nsmap[None]}
    return element.find('defualt:' + tag, namespaces=nsmap)


def get_tagged_event(event, element):
    """
    Given (event, element) returned by iterative parser
    returns a tuple of unchanged event and stripped
    elemnt's tag.
    """
    return tagged_event(event, tag_wo_ns(element))


# TODO move helpers related to db into db package

def get_table_colums(obj):
    """For an slqalchemy model or instance of a model (obj)
    returns a names of colums of a respective table in the db.
    They may be different for model attributes that correspond
    to those columns (but currently aren't)."""
    return set(obj.__table__.columns.keys())


def set_model_attr_from_xml_elem(instance, xml_elem):
    """
    Given 'instance' of an SQLAlchemy model and xml_elem
    if that model has an attribute with the same naem as
    xml_elem's tag sets that attribute on an instance to
    xml_elem's text.
    """
    stripped_tag = tag_wo_ns(xml_elem)
    if stripped_tag in get_table_colums(instance):
        setattr(instance, stripped_tag, xml_elem.text)


def set_revision_text(rev_instance, xml_text):
    text_size = 0 if xml_text.text is None else len(xml_text.text)
    text_obj = Text(text_size=text_size)
    rev_instance.text = text_obj


def set_revision_contributor(rev_instance, xml_contributor):
    ip = find_in_default_ns(xml_contributor, 'ip')
    id_ = find_in_default_ns(xml_contributor, 'id')
    if ip is not None:
        rev_instance.user_ip = ip.text
    elif id_ is not None:
        rev_instance.user_id = id_.text


def process_page_xml(xml_eater):
    """
    xml_eater is an iterative parser returned by etree.iterparse
    it should be at the point when ('start', 'page') event have
    just been triggered. The function consumes it until the Page
    object is fully built (except revisions).
    Returns constructed page.
    """
    trigger_events = {
        tagged_event(event, tag): False for event, tag in
        (('end', 'id'), ('end', 'title'), ('end', 'ns'))
    }
    page = Page()
    for event, element in xml_eater:
        tagged_event_ = get_tagged_event(event, element)
        if tagged_event_ in trigger_events:
            set_model_attr_from_xml_elem(page, element)
            trigger_events[tagged_event_] = True
        if all(trigger_events.values()):
            return page


def process_revision_xml(xml_eater):
    revision = Revision()
    end_handlers = {
        elem_tag: set_revision_contributor for elem_tag in
        ('id', 'comment', 'timestamp', 'parentid', 'comment', )
    }
    end_handlers['contributor'] = set_revision_contributor
    end_handlers['text'] = set_revision_text
    for event, element in xml_eater:
        if event == 'end':
            stripped_tag = tag_wo_ns(element)
            if stripped_tag == 'revision':
                element.clear()
                return revision
            elif stripped_tag in end_handlers:
                end_handlers[stripped_tag](
                    revision, element
                )


def parse_xml(xml_file, session_maker, page_limit=None):
    session = session_maker()
    xml_eater = etree.iterparse(xml_file, ('start', 'end'))
    page_counter = 0
    for event, element in xml_eater:
        tagged_event_ = get_tagged_event(event, element)
        if tagged_event_ == tagged_event('start', 'page'):
            page = process_page_xml(xml_eater)
            session.add(page)
        elif tagged_event_ == tagged_event('start', 'revision'):
            revision = process_revision_xml(xml_eater)
            page.revisions.append(revision)
        elif tagged_event_ == tagged_event('end', 'page'):
            page_title = find_in_default_ns(element, 'title').text
            try:
                session.commit()
                session.expunge_all()
                page_counter += 1
                LOGGER.info('Finished processing page: %s', page_title)
            except Exception:
                session.rollback()
                LOGGER.exception('Error while parsing element: %s', page_title)
            finally:
                element.clear()
                # clear previous elements
                while element.getprevious() is not None:
                    del element.getparent()[0]
        if page_limit and page_counter >= page_limit:
            return


if __name__ == '__main__':
    file_path = (
        '/media/storage/zlira/wiki/dumps/'
        'ukwiki-20161001-pages-meta-history.xml'
    )
    arg_parser = get_arg_parser()
    args = arg_parser.parse_args()
    with open(file_path, 'rb') as xml_file:
        parse_xml(
            xml_file,
            session_maker=Session if not args.test else TestSession,
            page_limit=args.page_number,
        )
