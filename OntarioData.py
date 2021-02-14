import os
import requests
from requests_html import HTML
import pandas as pd
import numpy as np
import mysql.connector
from sqlalchemy import create_engine


def get_cities():

    ontario_cities = [
        {'city': 'Belleville', 'station_id': 54012},
        {'city': 'Grand Bend', 'station_id': 15020},
        {'city': 'Guelph', 'station_id': 28028},
        {'city': 'Hamilton', 'station_id': 29000},
        {'city': 'Kingston', 'station_id': 52023},
        {'city': 'Kitchener', 'station_id': 26060},
        {'city': 'London', 'station_id': 15026},
        {'city': 'Oshawa', 'station_id': 45027},
        {'city': 'Ottawa', 'station_id': 51001},
        {'city': 'Parry Sound', 'station_id': 49005},
        {'city': 'Port Stanley', 'station_id': 16015},
        {'city': 'Sudbury', 'station_id': 77233},
        {'city': 'Thunder Bay', 'station_id': 63203},
        {'city': 'Toronto', 'station_id': 31129},
        {'city': 'Windsor', 'station_id': 12008}
    ]

    return ontario_cities


def scrape_city_data(station_id, year):

    url = f"http://www.airqualityontario.com/aqhi/search.php?stationid={station_id}&show_day=0&start_day=1&start_month=1&start_year={year}&submit_search=Get+AQHI+Readings"

    r = requests.get(url)
    if r.status_code == 200:
        html_text = r.text

    r_html = HTML(html=html_text)

    return r_html


def parse_data_to_df(r_html):

    table_class = ".resourceTable"
    r_table = r_html.find(table_class)

    table_data = []
    header_names = []

    if len(r_table) == 1:
        parsed_table = r_table[0]
        rows = parsed_table.find("tr")

        header_row = rows[0]
        header_cols = header_row.find('th')
        header_names = [x.text for x in header_cols]
        header_names.insert(3, 'Graph')

        for row in rows[1:]:
            cols = row.find("td")
            row_data = []
            for col in cols:
                row_data.append(col.text)
            table_data.append(row_data)

    df = pd.DataFrame(table_data, columns=header_names)

    return df


def df_handler():
    ontario_cities = get_cities()

    df = pd.DataFrame()

    for ontario_city in ontario_cities:
        city_r_html = scrape_city_data(ontario_city['station_id'], 2020)
        city_df = parse_data_to_df(city_r_html)
        city_df['station_id'] = ontario_city['station_id']
        city_df.replace('N/A', np.nan, inplace=True)
        city_df['AQHI'].fillna(
            city_df['AQHI'].value_counts().index[0], inplace=True)

        df = df.append(city_df, ignore_index=True)

    df = df.drop(['Graph', 'Time', 'Category'], 1)

    return df


def df_to_db(df):

    db_pass = os.getenv('DB_PASS')

    engine = create_engine(
        f'mysql+mysqlconnector://root:{db_pass}@localhost:3306/Air_Quality', echo=False)
    df.to_sql(name='Ontario AQHI Readings', con=engine,
              if_exists='append', index=False)
    # print(forecast_df)


def get_ontario_data():
    try:
        df = df_handler()
        df_to_db(df)
        print('Yay')
    except:
        print('Error')


if __name__ == '__main__':
    get_ontario_data()
