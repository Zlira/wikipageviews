from os import path
import sys

from sqlalchemy.sql import text
import yaml

from db.db_conf import engine
from db.db_conf import TestSession
from db.wiki_tables import Page

TOPICS_FILE = path.join(path.dirname(
    path.realpath(__file__)), 'topics.yml'
)

# TODO don't follow hidden categories!

def get_topics_two_way_dicts():
    with open(TOPICS_FILE) as topics_file:
        topics_to_cat = yaml.load(topics_file)
    cat_id_to_topic = {}
    for topic, cats in topics_to_cat.items():
        for cat_name, cat_id in cats:
            # fliter out unparsed categories for now
            if cat_id != '?':
                cat_id_to_topic[int(cat_id)] = topic
    return topics_to_cat, cat_id_to_topic


# TODO stop duplicating this code!
def get_page_id_by_title(title):
    session = TestSession()
    return (session.query(Page.id)
            .filter(Page.title == title)
            .one())


def get_article_topic(article_id, cat_topic_dict, max_depth=11):
    query = text(
        "select id from "
        "wiki_test.pages join ("
        "  select cl_from, concat('Категорія:', replace(cl_to, '_', ' ')) "
        "  as cat_to_name "
        "  from wiki.categorylinks) as catlinks "
        "on pages.title  = catlinks.cat_to_name "
        "where catlinks.cl_from in :id_list"
    )
    ids_list = (article_id, )
    with engine.connect() as conn:
        for _ in range(max_depth):
            rs = conn.execute(query, id_list=tuple(ids_list)).fetchall()
            ids_list = set(r[0] for r in rs)
            if cat_topic_dict.keys() & ids_list:
                return tuple(cat_topic_dict[id_] for id_ in
                             cat_topic_dict.keys() & ids_list)
    return ()


if __name__ == '__main__':
    topics_to_cat, cat_id_to_topic = get_topics_two_way_dicts()
    page_id = get_page_id_by_title(sys.argv[1])[0]
    print(get_article_topic(page_id, cat_id_to_topic))
