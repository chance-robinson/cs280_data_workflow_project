from models.config import Session #You would import this from your config file
from models.countries import Countries
from models.country_totals import Country_Totals
import requests
import json
import time
import datetime

session = Session()
q = session.query(Countries)
statuses = ['confirmed', 'deaths']
countries = requests.get(f"https://api.covid19api.com/countries").json()
num_countries = 2*len(countries)
for country_name in countries:
    if (q.filter(Countries.iso2 == country_name['ISO2']) and (q.all())) and not(q.filter(Countries.country==country_name['Country']).first() == None):
        q = q.filter(Countries.country==country_name['Country'])
        record = q.one()
        record.country = country_name['Country']
        record.slug = country_name['Slug']
        recordiso2 = country_name['ISO2']
    else:
        country = Countries(
                    country = country_name['Country'],
                    slug = country_name['Slug'],
                    iso2 = country_name['ISO2']
                )
        session.add(country)
session.commit()
session.close()
time.sleep(3)


timed_out = []

num_at = 0
for country_name in countries:
    for status in statuses:
        try:
            session = Session()
            country_total = requests.get(f"https://api.covid19api.com/country/{country_name['Slug']}/status/{status}?from=2020-03-01T00:00:00Z&to=2022-03-01T00:00:00Z").json()
            for datapoint in country_total:
                country_datapoint = Country_Totals(
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
            session.commit()
            session.close()
        except:
            print(f"Timed out, skipping: {country_name['Slug']}:{status} for later")
            timed_out.append([country_name['Slug'], status])
            pass
        num_at += 1
        print(f"Progress: {num_at} | {num_countries}")
        time.sleep(3)
        
# num_at = 0
# for country_name in countries:
#     for status in statuses:
#         try:
#             session = Session()
#             q = session.query(Country_Totals)
#             country_total = requests.get(f"https://api.covid19api.com/country/{country_name['Slug']}/status/{status}?from=2020-03-01T00:00:00Z&to=2022-03-01T00:00:00Z").json()
#             for datapoint in country_total:
#                 if (q.filter(Country_Totals.datetime == datapoint['Date']) and q.filter(Country_Totals.status == datapoint['Status']) and q.filter(Country_Totals.country_id == datapoint['CountryCode']) and (q.all())) and not(q.filter(Country_Totals.datetime==datapoint['Date']).first() == None):
#                     continue
#                 else:
#                     country_datapoint = Country_Totals(
#                                 country_id = datapoint['CountryCode'],
#                                 province = datapoint['Province'],
#                                 city = datapoint['City'],
#                                 city_code = datapoint['CityCode'],
#                                 lat = datapoint['Lat'],
#                                 long = datapoint['Lon'],
#                                 cases = datapoint['Cases'],
#                                 status = datapoint['Status'],
#                                 datetime = datapoint['Date']
#                             )
#                     session.add(country_datapoint)
#             session.commit()
#             session.close()
#         except:
#             print(f"Timed out, skipping: {country_name['Slug']}:{status} for later")
#             timed_out.append([country_name['Slug'], status])
#             pass
#         num_at += 1
#         print(f"Progress: {num_at} | {num_countries}")
#         time.sleep(3)

print(f'All that timed out: {timed_out}')
num_at = 0
num_timeouts = len(timed_out)
for values in timed_out:
    time_sleep = 10
    country_total = None
    while country_total is None:
        try:
            session = Session()
            country_total = requests.get(f"https://api.covid19api.com/country/{values[0]}/status/{values[1]}?from=2020-03-01T00:00:00Z&to=2022-03-01T00:00:00Z").json()
            for datapoint in country_total:
                country_datapoint = Country_Totals(
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
            session.commit()
            session.close()
            time.sleep(3)
        except Exception as e:
            print(f"Error: {e}")
            print(f"Error: Waiting {time_sleep} seconds and trying again")
            time.sleep(time_sleep)
            pass
    num_at += 1
    print(f"Timed_out Progress: {num_at} | {num_timeouts}")
    time.sleep(5)

start_date = datetime.datetime.strptime("2020-03-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ")
end_date = datetime.datetime.strptime("2022-03-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ")

current_date = start_date
weeks = []
while current_date <= end_date:
    start_of_week = current_date
    end_of_week = current_date + datetime.timedelta(weeks=1)
    if end_of_week >= end_date:
        end_of_week = end_date + datetime.timedelta(days=1)
    weeks.append((start_of_week.strftime("%Y-%m-%dT%H:%M:%SZ"), end_of_week.strftime("%Y-%m-%dT%H:%M:%SZ")))
    current_date = end_of_week + datetime.timedelta(days=1)

print(f'US Weeks Progress: 0 | {2*len(weeks)}')
num_at = 0
for status in statuses:
    for week in weeks:
        time_sleep = 10
        country_total = None
        while country_total is None:
            try:
                session = Session()
                country_total = requests.get(f"https://api.covid19api.com/country/united-states/status/{status}?from={week[0]}&to={week[1]}").json()
                for datapoint in country_total:
                    country_datapoint = Country_Totals(
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
                session.commit()
                session.close()
                time.sleep(3)
            except Exception as e:
                print(f"Error: {e}")
                print(f"Error: Waiting {time_sleep} seconds and trying again")
                time.sleep(time_sleep)
                pass
        num_at += 1
        print(f"US Weeks Progress: {num_at} | {2*len(weeks)}")
        time.sleep(5)