import pandas as pd
import requests
import json
import os
import sys
import pytz
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Importando DatabaseOps para operações de banco de dados
from utils.database_operations import DatabaseOps  

class IntegracaoSilver:
    """
    Classe para integração e transformação de dados da camada Silver.

    Attributes:
        today (datetime.date): Data atual.
        ref_month (int): Mês de referência.
        ref_day (int): Dia de referência.
    """

    def __init__(self):
        """
        Método construtor da classe IntegracaoSilver.
        """
        self.today = datetime.now().date()  # Define a data atual
        self.ref_month = self.today.month  # Define o mês de referência
        self.ref_day = self.today.day  # Define o dia de referência

    def silver_city_information(self):
        """
        Função para extrair e transformar informações das cidades da camada Bronze.

        Returns:
            DataFrame: DataFrame contendo informações das cidades.
        """
        BRONZE_DIR = os.path.join(fr'..\data\bronze\{self.ref_month}\{self.ref_day}')
        dir_city = pd.read_parquet(os.path.join(BRONZE_DIR, 'city_information.parquet'))

        # Convertendo os segundos para objetos datetime
        dir_city['sunrise'] = pd.to_datetime(dir_city['sunrise'], unit='s').dt.strftime('%H:%M:%S')
        dir_city['sunset'] = pd.to_datetime(dir_city['sunset'], unit='s').dt.strftime('%H:%M:%S')

        # Convertendo para o fuso horário de São Paulo
        fuso_horario_atual = pytz.timezone('America/Sao_Paulo')
        dir_city['timezone'] = datetime.now(fuso_horario_atual).strftime('%H:%M:%S')
        dir_city['ref'] = datetime.now().strftime('%Y/%m')

        return dir_city

    def silver_temperatures_information(self):
        """
        Função para extrair e transformar informações de temperatura da camada Bronze.

        Returns:
            DataFrame: DataFrame contendo informações de temperatura.
        """
        BRONZE_DIR = os.path.join(fr'..\data\bronze\{self.ref_month}\{self.ref_day}')
        dir_temperatures = pd.read_parquet(os.path.join(BRONZE_DIR, 'temperatures_information.parquet'))

        dir_temperatures['temp_celsius'] = round(dir_temperatures['temp'] - 273.15, 2)
        dir_temperatures['temp_fahrenheit'] = round((dir_temperatures['temp_celsius'] * 9/5) + 32, 2)

        dir_temperatures.drop('temp', axis=1, inplace=True)

        dir_temperatures['temp_min_celsius'] = round(dir_temperatures['temp_min'] - 273.15, 2)
        dir_temperatures['temp_max_celsius'] = round(dir_temperatures['temp_max'] - 273.15, 2)

        dir_temperatures['temp_min_fahrenheit'] = round((dir_temperatures['temp_min_celsius'] * 9/5) + 32, 2)
        dir_temperatures['temp_max_fahrenheit'] = round((dir_temperatures['temp_max_celsius'] * 9/5) + 32, 2)

        dir_temperatures['feels_like_celsius'] = round(dir_temperatures['feels_like'] - 273.15, 2)
        dir_temperatures['feels_like_fahrenheit'] = round((dir_temperatures['feels_like_celsius'] * 9/5) + 32, 2)

        dir_temperatures['dt_ingestao'] = datetime.now().strftime('%Y-%m-%d')

        df_resultado = dir_temperatures[['id_city','temp_celsius','temp_fahrenheit', 'feels_like_celsius','feels_like_fahrenheit', 'temp_min_celsius','temp_max_celsius','temp_min_fahrenheit','temp_max_fahrenheit','pressure','id','dt_ingestao']]
        return df_resultado

    def silver_weather_of_the_day(self):
        """
        Função para extrair e transformar informações meteorológicas do dia da camada Bronze.

        Returns:
            DataFrame: DataFrame contendo informações meteorológicas do dia.
        """
        BRONZE_DIR = os.path.join(fr'..\data\bronze\{self.ref_month}\{self.ref_day}')
        dir_weather = pd.read_parquet(os.path.join(BRONZE_DIR, 'weather_of_day.parquet'))

        # Remover o deslocamento do fuso horário
        fuso_horario = pytz.timezone('America/Sao_Paulo')
        dir_weather['date'] = dir_weather['dt'].apply(lambda x: datetime.fromtimestamp(x, fuso_horario).strftime('%Y-%m-%d %H:%M:%S'))
        dir_weather['rain'].fillna(0, inplace=True)

        dir_weather.drop('dt', axis=1, inplace=True)

        return dir_weather

    def silver_wind_information(self):
        """
        Função para extrair e transformar informações de vento da camada Bronze.

        Returns:
            DataFrame: DataFrame contendo informações de vento.
        """
        BRONZE_DIR = os.path.join(fr'..\data\bronze\{self.ref_month}\{self.ref_day}')
        dir_wind = pd.read_parquet(os.path.join(BRONZE_DIR, 'wind_information.parquet'))

        dir_wind['speed_km_h'] = round(dir_wind['speed'] * 3.6, 2)
        dir_wind['speed_mph'] = round(dir_wind['speed'] * 2.23694, 2)
        dir_wind['gust_km_h'] = round(dir_wind['gust'] * 3.6, 2)
        dir_wind['gust_mph'] = round(dir_wind['gust'] * 2.23694, 2)

        dir_wind[['speed_km_h', 'speed_mph', 'gust_km_h', 'gust_mph']] = dir_wind[['speed_km_h', 'speed_mph', 'gust_km_h', 'gust_mph']].fillna(0)

        dir_wind.drop(['speed','deg','gust'], axis=1, inplace=True)

        return dir_wind

    def connect_databases(self):
        """
        Método para conectar ao banco de dados.
        """
        global database, database_connection

        database = DatabaseOps()
        database_connection = database.connect_db()

    def insert_database(self, df, schema, table):
        """
        Método para inserir dados no banco de dados.

        Args:
            df (DataFrame): DataFrame contendo os dados a serem inseridos.
            schema (str): Nome do schema no banco de dados.
            table (str): Nome da tabela no banco de dados.

        Returns:
            bool: True se a inserção for bem-sucedida, False caso contrário.
        """
        try:
            database.insert(dataframe=df, schema=schema, table=table, if_exists='append')
            return True 
        except Exception as e:
            print(f"Erro durante a inserção no banco de dados: {e}")
            return False 

    def pipeline(self):
        """
        Método para executar o pipeline de integração de dados.

        Returns:
            bool: True se o pipeline for concluído com sucesso, False caso contrário.
        """
        try:
            # Obter e inserir informações das cidades
            silver_city = self.silver_city_information()
            self.insert_database(silver_city, 'silver', 'city_information')

            # Obter e inserir informações de temperatura
            silver_temperature = self.silver_temperatures_information()
            self.insert_database(silver_temperature, 'silver', 'temperatures_information')

            # Obter e inserir informações meteorológicas do dia
            silver_weather = self.silver_weather_of_the_day()
            self.insert_database(silver_weather, 'silver', 'weather_of_the_day')

            # Obter e inserir informações de vento
            silver_wind = self.silver_wind_information()
            self.insert_database(silver_wind, 'silver', 'wind_information')

            return True
        except Exception as e:
            print(f"Erro durante a inserção no banco de dados: {e}")
            return False 
