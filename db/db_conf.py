# TODO decide where all this shit should reside
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine(
    'mysql+pymysql://wiki:ner0_ceasar@localhost/wiki?'
    'charset=utf8'
)
Session = sessionmaker(bind=engine)
