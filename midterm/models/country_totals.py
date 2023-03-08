from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float, BigInteger

Base = declarative_base()

class Country_Totals(Base):
    __tablename__ = "country_totals"
    id = Column(Integer, primary_key=True, not_null=True)
    country_id = Column(Integer, not_null=True)
    province = Column(String, not_null=False)
    city = Column(String, not_null=False)
    city_code = Column(String, not_null=False)
    lat = Column(String, not_null=True)
    long = Column(String, not_null=True)
    cases = Column(Integer, not_null=True)
    status = Column(String, not_null=True)
    datetime = Column(Date, not_null=True)

    def __repr__(self) -> str:
        return f"Country_Totals(id={self.id}, country_id={self.country_id}, province={self.province}, city={self.city}, city_code={self.city_code}, lat={self.lat}, long={self.long}, cases={self.cases}, status={self.status}, datetime={self.datetime})"