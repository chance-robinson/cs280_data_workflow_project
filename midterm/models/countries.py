from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

Base = declarative_base()

class Countries(Base):
    __tablename__ = "countries"
    id = Column(Integer, primary_key=True, not_null=True)
    country = Column(String, not_null=True)
    slug = Column(String, not_null=True)
    iso2 = Column(String, not_null=True)

    def __repr__(self) -> str:
        return f"Countries(id={self.id}, country={self.country}, slug={self.slug}, iso2={self.iso2})"