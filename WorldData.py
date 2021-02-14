import os
from urllib.parse import urlencode
import requests
import pandas as pd
import time
import json
import datetime
import mysql.connector
from sqlalchemy import create_engine


def get_cities_info():

    cities_info = [{"city": "Ottawa", "state": "Ontario", "country": "Canada", "lat": 45.4322, "lon": -75.6764, "idx": 23},
                   {"city": "Dhaka", "state": "Dhaka", "country": "Bangladesh",
                       "lat": 23.7959872, "lon": 90.4232701, "idx": 8781},
                   {"city": "Islamabad", "state": "Islamabad", "country": "Pakistan",
                       "lat": 33.697612, "lon": 72.971079, "idx": 11739},
                   {"city": "Ulaanbaatar", "state": "Ulaanbaatar", "country": "Mongolia",
                       "lat": 47.86419619999999, "lon": 106.7662644, "idx": 2576},
                   {"city": "Kabul", "state": "Kabul", "country": "Afghanistan",
                       "lat": 34.53536411, "lon": 69.1798542, "idx": 12384},
                   {"city": "New Delhi", "state": "Delhi", "country": "India",
                       "lat": 28.656, "lon": 77.231, "idx": 7024},
                   {"city": "Jakarta", "state": "Jakarta", "country": "Indonesia",
                       "lat": -6.236704, "lon": 106.79324, "idx": 8648},
                   {"city": "Manama", "state": "Capital Governorate", "country": "Bahrain",
                       "lat": 26.204689, "lon": 50.570833, "idx": 9288},
                   #  {"city": "kathmandu", "state":"central region", "country":"nepal", "lat":27.7, "lon":85.31},
                   {"city": "Tashkent", "state": "Toshkent Shahri", "country": "Uzbekistan",
                       "lat": 41.26465, "lon": 69.21627, "idx": 11219},
                   {"city": "Baghdad", "state": "Baghdad", "country": "Iraq",
                       "lat": 33.34058, "lon": 44.40088, "idx": 11452},
                   #  {"city": "nassau", "state":"new providence", "country":"bahamas", "lat":25.03, "lon":-77.53},
                   #  {"city": "charlotte amalie", "state":"saint thomas island", "country":"u.s. virgin islands", "lat":18.322151, "lon":-64.851688},
                   {"city": "Reykjavik", "state": "Capital Region", "country": "Iceland",
                       "lat": 64.13548, "lon": -21.89541, "idx": 8344},
                   {"city": "Helsinki", "state": "Uusimaa", "country": "Finland",
                       "lat": 60.169643, "lon": 24.939261, "idx": 4909},
                   {"city": "Stockholm", "state": "Stockholm", "country": "Sweden",
                       "lat": 59.33258000000001, "lon": 18.0649, "idx": 10012},
                   {"city": "Oslo", "state": "Oslo", "country": "Norway",
                       "lat": 59.9374561, "lon": 10.683366, "idx": 2654},
                   {"city": "Canberra", "state": "ACT", "country": "Australia",
                       "lat": -35.28346, "lon": 149.12807, "idx": 12492},
                   {"city": "Tallinn", "state": "Harjumaa", "country": "Estonia",
                       "lat": 59.43107, "lon": 24.760317, "idx": 9460},
                   {"city": "Wellington City", "state": "Wellington", "country": "New Zealand",
                       "lat": -41.293622, "lon": 174.771918, "idx": 8525},
                   {"city": "Washington", "state": "Washington D.C.", "country": "USA",
                       "lat": 38.895683, "lon": -76.958089, "idx": 5336},
                   {"city": "Beijing", "state": "Beijing", "country": "China",
                       "lat": 39.941674, "lon": 116.462153, "idx": 451},
                   {"city": "London", "state": "England", "country": "UK", "lat": 51.456357, "lon": 0.040725, "idx": 7957}]
    # {"city": "", "state":"", "country":""},

    return cities_info


def airvisual_requester(city_info, air_visual_key):

    base_url = 'https://api.airvisual.com/v2'
    query_params = urlencode({"city": city_info["city"], "state": city_info["state"],
                              "country": city_info["country"], "key": air_visual_key})

    lookup_url = f'{base_url}/city?{query_params}'
    response = requests.get(lookup_url)
    if response.status_code != 200:
        print('yup error', city_info)
    data = response.json()

    return data["data"]


def airvisual(city_info, air_visual_key):

    airvisual_data = airvisual_requester(city_info, air_visual_key)
    row_df = pd.json_normalize(airvisual_data)
    return row_df


def waqi_requester(city_info, waqi_token):

    base_url = 'https://api.waqi.info'
    lookup_url = f'{base_url}/feed/geo:{city_info["lat"]};{city_info["lon"]}/?token={waqi_token}'
    response = requests.get(lookup_url)
    data = response.json()

    return data["data"]


def waqi_data_handler(row_df):
    """Save only the next day forecast for each pollution type"""

    today_str = row_df['time']['s']
    today_date_obj = datetime.datetime.strptime(today_str, '%Y-%m-%d %H:%M:%S')
    tomorrow_date_obj = today_date_obj.date() + datetime.timedelta(days=1)
    tomorrow_str = tomorrow_date_obj.strftime("%Y-%m-%d")

    forecast_types = ["o3", "pm10", "pm25", "uvi"]

    for forecast_type in forecast_types:
        row_df['forecast']['daily'][forecast_type] = [
            x for x in row_df["forecast"]["daily"][forecast_type] if x["day"] == tomorrow_str][0]

    row_df = pd.json_normalize(row_df)

    return row_df


def waqi(city_info, waqi_token):

    waqi_data = waqi_requester(city_info, waqi_token)
    row_df = waqi_data_handler(waqi_data)
    return row_df


def get_cities_info_two():
    cities_info = [
        {"city": "Washington", "state": "Washington D.C.", "country": "USA",
         "lat": 38.895683, "lon": -76.958089, "idx": 5336},
        {"city": "Beijing", "state": "Beijing", "country": "China",
         "lat": 39.941674, "lon": 116.462153, "idx": 451},
        {"city": "London", "state": "England", "country": "UK", "lat": 51.456357, "lon": 0.040725, "idx": 7957}]
    return cities_info


def api_to_dataframe():

    cities_info = get_cities_info()

    air_visual_key = os.getenv('AIR_VISUAL_KEY')
    waqi_token = os.getenv('WAQI_TOKEN')

    df = pd.DataFrame()
    counter = 0

    """AirVisual API can only handle about 8 calls in a given minute. After 8 calls have been made, 
    the calls will halt for 2 minutes before continuing."""

    for i in range(len(cities_info)):
        if counter % 8 == 0:
            time.sleep(120)
        airvisual_row = airvisual(cities_info[i], air_visual_key)
        waqi_row = waqi(cities_info[i], waqi_token)
        row = airvisual_row.join(waqi_row)
        df = df.append(row, ignore_index=True)
        counter += 1

    city_df = pd.DataFrame(cities_info)
    df = pd.merge(city_df, df, how="inner", on="city")

    return df


def forecast_dataframe(df):

    forecast_types = ["o3", "pm10", "pm25", "uvi"]
    forecast_df = pd.DataFrame(columns=[
                               'forecast_day', 'avg', 'max', 'min', 'idx', 'current_time', 'forecast_type'])
    for forecast_type in forecast_types:
        forecast_type_data = df[[f'forecast.daily.{forecast_type}.day', f'forecast.daily.{forecast_type}.avg',
                                 f'forecast.daily.{forecast_type}.max', f'forecast.daily.{forecast_type}.min', 'idx_x', 'time.s']]
        forecast_type_data = forecast_type_data.assign(
            forecast_type=forecast_type)
        forecast_type_data.columns = [
            'forecast_day', 'avg', 'max', 'min', 'idx', 'current_time', 'forecast_type']
        forecast_df = forecast_df.append(forecast_type_data, ignore_index=True)
    forecast_df = forecast_df[['idx', 'current_time',
                               'forecast_type', 'forecast_day', 'avg', 'max', 'min']]
    return forecast_df


def current_weather_dataframe(df):

    current_weather_df = df[['idx_x', 'time.s', 'current.weather.tp', 'current.weather.pr',
                             'current.weather.hu', 'current.weather.ws', 'current.weather.wd',
                             'current.weather.ic']]
    current_weather_df.columns = ['idx', 'current_time', 'temperature(C)', 'atmospheric_pressure(hPa)', 'humidity(%)',
                                  'wind_speed(m/s)', 'wind_direction', 'weather_icon']
    return current_weather_df


def current_pollution_dataframe(df):

    current_pollution_df = df[['idx_x', 'time.s', 'current.pollution.aqius',
                               'current.pollution.mainus', 'current.pollution.aqicn',
                               'current.pollution.maincn', 'dominentpol', 'aqi', 'iaqi.co.v',
                               'iaqi.no2.v', 'iaqi.o3.v', 'iaqi.pm10.v', 'iaqi.pm25.v', 'iaqi.so2.v']]
    current_pollution_df.columns = ['idx', 'current_time', 'aquis', 'mainus', 'aqicn', 'maincn',
                                    'dominentpol', 'aqi', 'aqi_co', 'aqi_no2', 'aqi_o3', 'aqi_pm10', 'aqi_pm25', 'aqi_so2']
    current_pollution_df.replace(
        {"p1": "pm10", 'p2': "pm25", "n2": "no2", "s2": "so2"}, inplace=True)
    return current_pollution_df


def dataframes_to_db(df):

    db_pass = os.getenv('DB_PASS')

    engine = create_engine(
        f'mysql+mysqlconnector://root:{db_pass}@localhost:3306/Air_Quality', echo=False)

    forecast_df = forecast_dataframe(df)
    forecast_df.to_sql(name='Forecast', con=engine,
                       if_exists='append', index=False)
    # # print(forecast_df)

    current_weather_df = current_weather_dataframe(df)
    current_weather_df.to_sql(name='Current Weather',
                              con=engine, if_exists='append', index=False)
    # # print(current_weather_df)

    current_pollution_df = current_pollution_dataframe(df)
    current_pollution_df.to_sql(
        name='Current Pollution', con=engine, if_exists='append', index=False)
    # print(current_pollution_df)


def api_to_db():
    try:
        df = api_to_dataframe()
        dataframes_to_db(df)
        print('yay')
    except:
        print('error')


if __name__ == '__main__':
    api_to_db()
