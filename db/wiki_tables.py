from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy.types import DateTime
from sqlalchemy.types import Text as TextType


Base = declarative_base()


class Page(Base):
    __tablename__ = 'pages'

    id = Column(Integer, primary_key=True)
    title = Column(TextType)
    ns = Column(Integer)  # namespace

    # relations
    revisions = relationship(
        "Revision", back_populates='page'
    )

    def __repr__(self):
        return 'Page {title} ({id}) in namespace {ns}'.format(
            title=self.title, id=self.id, ns=self.ns,
        )


class Revision(Base):
    __tablename__ = 'revisions'

    id = Column(Integer, primary_key=True)
    comment = Column(TextType)
    # TODO maybe there's special type for ips
    user_ip = Column(TextType)
    timestamp = Column(DateTime)

    # foreign keys
    page_id = Column(Integer, ForeignKey('pages.id'))
    # TODO WARNING I've droped this foreing key in db becuase
    # it caused errors during parsing should chage that later
    parentid = Column(Integer, ForeignKey('revisions.id'))
    user_id = Column(Integer)
    text_size = Column(Integer)

    # relations
    page = relationship(
        "Page", back_populates='revisions'
    )
    child = relationship('Revision', uselist=False)

    def __repr__(self):
        return 'Revision #{id} of page #{page_id} on {timestamp}'.format(
            id=self.id, page_id=self.page_id, timestamp=self.timestamp,
        )


# existing classes
# TODO make this work with created classes!
# maybe for start just use another declarative base
# tables cannot be mapped if they don't have primary key
# so I've added primary keys to some of them
# Category = Base.classes.category
# CategoryLinks = Base.classes.categorylinks
# ExternlaLinks = Base.classes.externallinks
# GeoTagse = Base.classes.geo_tags
# ImageLinks = Base.classes.imagelinks
