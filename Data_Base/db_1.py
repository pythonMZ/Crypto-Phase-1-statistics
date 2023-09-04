import glob
import os

from sqlalchemy import create_engine, Integer, String, ForeignKey, Table, Date, Time, Text, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

import pandas as pd

connection_string = "mysql+mysqlconnector://user1:pscale_pw_abc123@us-east.connect.psdb.cloud:3306/sqlalchemy"
engine = create_engine(connection_string)
Base = declarative_base()

coin_tag = Table('student_course', Base.metadata,
                 Column('coin_id', Integer, ForeignKey('coin.id')),
                 Column('tag_id', Integer, ForeignKey('tag_id')))

coin_exchange = Table('coin_exchange', Base.metadata,
                      Column('coin_id', Integer, ForeignKey('coin.id')),
                      Column('exchange_id', Integer, ForeignKey('exchange_id')))


class Coin(Base):
    __tablename__ = 'coin'

    rank = Column(Integer, primary_key=True)
    name = Column(Text)
    symbol = Column(Text)
    main_link = Column('mainLink', Text)
    historical_link = Column('historicalLink', Text)

    coin_histories = relationship('CoinHistory', back_populate='coin')
    exchanges = relationship('Exchange', secondary=coin_exchange, back_populates='coins')
    tags = relationship('Tag', secondary=coin_tag, back_populates='coins')


class CoinHistory(Base):
    __tablename__ = "coin_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    coinId = Column(Integer, ForeignKey("coins.id"), nullable=False)
    marketCap = Column(Integer, nullable=False)
    volume = Column("volume(24)", Integer, nullable=False)
    open = Column(Integer)
    high = Column(Integer)
    low = Column(Integer)
    close = Column(Integer)
    circulatingSupply = Column(Integer)
    topExchange = Column(Integer)
    timeLow = Column(Time)
    timeHigh = Column(Time)
    date = Column(Date)

    coin = relationship('Coin', back_populate='coin_histories')


class Exchange(Base):
    __tablename__ = 'exchange'

    id = Column(Integer, primary_key=True, autoincrement="auto")
    name = Column("ExchangeName", Text)
    pair = Column("ExchangePair", Text)

    exchanges = relationship('Coin', secondary=coin_exchange, back_populates='exchanges')


class GitHub(Base):
    __tablename__ = 'github_data'

    id = Column(Integer, primery_key=True, autoincrement="auto")
    coinId = Column(Integer, ForeignKey("coins.id"), nullable=False)
    gitHubLink = Column(Text, unique=True)


class Tag(Base):
    __tablename__ = 'tag'

    id = Column(Integer, primary_key=True, autoincrement="auto")
    coinId = Column(Integer, ForeignKey('coins.id'))
    name = Column(String, unique=True)

    exchanges = relationship('Coin', secondary=coin_tag, back_populates='tags')


Base.metadata.create_all(engine)


def create_table(folder_of_files, table_name):
    files = glob.glob(os.path.join(folder_of_files, "*.csv"))

    for file_name in files:

        df = pd.read_csv(file_name)

        if table_name == 'coin':
            df = df["rank", "name", "symbol", "mainLink", "HistoricalLink"]
            df.to_sql(name='coin', con=engine, if_exists='append')

        elif table_name == 'coin_history':
            df = df["timeHigh", "timeLow", "open", "high", "low", "close", "volume", "marketCap"]
            df["timeHigh"] = pd.to_datetime(df["timeHIgh"])
            df['date'] = df['timeHigh'].dt.strftime("%Y-%m-%d")
            df['time'] = df['timeHigh'].dt.strftime("%H:%M:%S")
            df["timeLow"] = pd.to_datetime(df["timeLow"])
            df['time'] = df['timeLow'].dt.strftime("%H:%M:%S")
            df.to_sql(name='coin_history', con=engine, if_exists='append')

        elif table_name == 'exchange':
            df.to_sql(name='exchange', con=engine, if_exists='append')

        elif table_name == 'tag':

            df.to_sql(name='tag', con=engine, if_exists='append')
        elif table_name == 'github_data':

            df.to_sql(name='github_data', con=engine, if_exists='append')
        else:
            print('table name is not correct')