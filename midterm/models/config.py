from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def create_dburl():
  return f"postgresql+psycopg2://postgres:robinson@0.0.0.0:5432/covid"

engine = create_engine(create_dburl())
Session = sessionmaker(bind=engine)