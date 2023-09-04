import glob
import os

from sqlalchemy import create_engine, Integer, String, column, ForeignKey, Table, Date, Time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

import pandas as pd

connection_string = "mysql+mysqlconnector://user1:pscale_pw_abc123@us-east.connect.psdb.cloud:3306/sqlalchemy"
engine = create_engine(connection_string)
Base = declarative_base()

coin_tag = Table('student_course', Base.metadata,
                 column('coin_id', Integer, ForeignKey('coin.id')),
                 column('tag_id', Integer, ForeignKey('tag_id')))

coin_exchange = Table('coin_exchange', Base.metadata,
                      column('coin_id', Integer, ForeignKey('coin.id')),
                      column('exchange_id', Integer, ForeignKey('exchange_id')))


class Coin(Base):
    __tablename__ = 'coin'

    id = column(Integer, primary_key=True, autoincrement=True)
    rank = column(Integer)
    name = column(String(255))
    symbol = column(String(50))
    main_link = column(String(255))
    historical_link = column(String(255))

    coin_histories = relationship('CoinHistory', back_populate='coin')
    exchanges = relationship('Exchange', secondary=coin_exchange, back_populates='coins')
    tags = relationship('Tag', secondary=coin_tag, back_populates='coins')


class CoinHistory(Base):
    __tablename__ = "coin_history"

    id = column(Integer, primary_key=True, autoincrement=True)
    coinId = column(Integer, ForeignKey("coins.id"), nullable=False)
    marketCap = column(Integer, nullable=False)
    volume = column("volume(24)", Integer, nullable=False)
    open = column(Integer)
    high = column(Integer)
    low = column(Integer)
    close = column(Integer)
    circulatingSupply = column(Integer)
    topExchange = column(Integer)
    timeLow = column(Time)
    timeHigh = column(Time)
    date = column(Date)

    coin = relationship('Coin', back_populate='coin_histories')


class Exchange(Base):
    __tablename__ = 'exchange'

    id = column(Integer, primary_key=True, autoincrement="auto")
    name = column("ExchangeName", String(255))
    pair = column("ExchangePair", String(255))

    exchanges = relationship('Coin', secondary=coin_exchange, back_populates='exchanges')


class GitHub(Base):
    __tablename__ = 'github_data'

    id = column(Integer, primery_key=True, autoincrement="auto")
    coinId = column(Integer, ForeignKey("coins.id"), nullable=False)
    gitHubLink = coinId(String, unique=True)


class Tag(Base):
    __tablename__ = 'tag'

    id = column(Integer, primary_key=True, autoincrement="auto")
    coinId = column(Integer, ForeignKey('coins.id'))
    name = column(String, unique=True)

    exchanges = relationship('Coin', secondary=coin_tag, back_populates='tags')


Base.metadata.create_all(engine)


def create_table(folder_of_files, table_name):
    files = glob.glob(os.path.join(folder_of_files, "*.csv"))

    for file_name in files:

        df = pd.read_csv(file_name)

        if table_name == 'coin':

            df.to_sql(name='coin', con=engine, if_exists='append')
        elif table_name == 'coin_history':
            df.to_sql(name='coin_history', con=engine, if_exists='append')
        elif table_name == 'exchange':
            df.to_sql(name='exchange', con=engine, if_exists='append')
        elif table_name == 'tag':
            df.to_sql(name='tag', con=engine, if_exists='append')
        elif table_name == 'github_data':
            df.to_sql(name='github_data', con=engine, if_exists='append')
        else:
            print('table name is not correct')
