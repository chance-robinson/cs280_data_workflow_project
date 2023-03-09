from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, BigInteger, DateTime

Base = declarative_base()

class Country_Totals(Base):
    __tablename__ = "country_totals"
    id = Column(Integer, primary_key=True, nullable=True)
    country_id = Column(String, nullable=True)
    province = Column(String, nullable=False)
    city = Column(String, nullable=False)
    city_code = Column(String, nullable=False)
    lat = Column(String, nullable=True)
    long = Column(String, nullable=True)
    cases = Column(Integer, nullable=True)
    status = Column(String, nullable=True)
    datetime = Column(DateTime, nullable=True)

    def __repr__(self) -> str:
        return f"Country_Totals(id={self.id}, country_id={self.country_id}, province={self.province}, city={self.city}, city_code={self.city_code}, lat={self.lat}, long={self.long}, cases={self.cases}, status={self.status}, datetime={self.datetime})"