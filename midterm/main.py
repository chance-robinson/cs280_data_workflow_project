from models.config import Session #You would import this from your config file
from models.countries import Countries
from models.country_totals import Country_Totals
import requests
import json
import time

session = Session()
statuses = ['confirmed', 'deaths']
countries = requests.get(f"https://api.covid19api.com/countries").json()
for country_name in countries:
    country = Countries(
                country = country_name['Country'],
                slug = country_name['Slug'],
                iso2 = country_name['ISO2']
            )
    session.add(country)
session.commit()
session.close()

session = Session()
for country_name in countries:
    for status in statuses:
        country_total = requests.get(f"https://api.covid19api.com/country/{country_name['Slug']}/status/{status}?from=2020-03-01T00:00:00Z&to=2022-03-01T00:00:00Z").json()
        for datapoint in country_total:
            country_datapoint = Countries(
                        country_id = datapoint['CountryCode'],
                        province = datapoint['Province'],
                        city = datapoint['City'],
                        city_code = datapoint['CityCode'],
                        lat = datapoint['Lat'],
                        long = datapoint['Lon'],
                        cases = datapoint['Cases'],
                        status = datapoint['Status'],
                        datetime = datapoint['Date']
                    )
            session.add(country_datapoint)
        time.sleep(5)
session.commit()
session.close()
