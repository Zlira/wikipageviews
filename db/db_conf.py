# TODO decide where all this shit should reside
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine_template = (
    'mysql+pymysql://wiki:ner0_ceasar@localhost/{db_name}?'
    'charset=utf8'
)
db_name = 'wiki'
test_db_name = 'wiki_test'
engine = create_engine(
    engine_template.format(db_name=db_name), echo=True
)
test_engine = create_engine(
    engine_template.format(db_name=test_db_name)
)

Session = sessionmaker(bind=engine)
TestSession = sessionmaker(bind=test_engine)
