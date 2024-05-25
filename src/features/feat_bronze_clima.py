import pandas as pd
import requests
import json
import os
import sys
import pytz
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Importar a classe DatabaseOps do módulo utils.database_operations
src_dir = os.path.join(os.getcwd().split('src')[0], 'src')
sys.path.insert(0, src_dir)

from utils.database_operations import DatabaseOps

src_dir = os.path.join(os.getcwd().split('src')[0], 'src','utils')
sys.path.insert(0, src_dir)

class ClimateData:
    """
    Classe para manipulação de dados climáticos.

    Attributes:
        today (datetime.date): Data atual.
        ref_month (int): Mês de referência.
        ref_day (int): Dia de referência.
        local_dir (str): Diretório local onde os dados serão armazenados.
    """
    def __init__(self):
        self.today = datetime.now().date()
        self.ref_month = self.today.month
        self.ref_day = self.today.day
        self.local_dir = None  # Diretório local onde os dados serão armazenados

    # Criação do diretório local para armazenar os dados meteorológicos
    def create_local_directory(self, ref_month, ref_day):
        """
        Função para criar o diretório local onde os dados meteorológicos serão armazenados.

        Args:
            ref_month (int): Mês de referência.
            ref_day (int): Dia de referência.

        Returns:
            str: Caminho completo para o diretório local criado.
        """
        BRONZE_DIR = r'..\data\bronze'
        local_dir = os.path.join(BRONZE_DIR, f'{ref_month}\{ref_day}')
        os.makedirs(local_dir, exist_ok=True)
        self.local_dir = local_dir  # Define o diretório local
        return local_dir

    # Carrega uma lista de cidades brasileiras para as quais os dados meteorológicos serão coletados
    def load_city_list(self):
        """
        Função para carregar uma lista de cidades brasileiras para coleta de dados meteorológicos.

        Returns:
            DataFrame: DataFrame contendo informações das cidades brasileiras.
        """
        with open("city.list.json", encoding='utf-8') as my_json:
            city_data = json.load(my_json)
            df_id_list = pd.DataFrame(city_data)

            df_id_list['id'] = df_id_list['id'].astype(int)
            br_citys = df_id_list[df_id_list['country'] == 'BR']  
            br_cidades = br_citys.sample(n=5)  # Seleciona aleatoriamente 5 cidades
            return br_cidades

    # Coleta os dados meteorológicos para as cidades selecionadas
    def fetch_weather_data(self, br_cidades):
        """
        Função para coletar dados meteorológicos para as cidades selecionadas.

        Args:
            br_cidades (DataFrame): DataFrame contendo informações das cidades brasileiras.

        Returns:
            DataFrame: DataFrame contendo os dados meteorológicos coletados.
        """
        API_CLIMA_KEY = os.getenv('API_CLIMA_KEY')  # Chave de API para acesso aos dados meteorológicos

        df_vazio = []

        for city_id in br_cidades['id']:
            url = f"https://api.openweathermap.org/data/2.5/weather?id={city_id}&appid={API_CLIMA_KEY}"
            response = requests.get(url)

            if response.status_code == 200:
                city_clima_date = response.json()
                df_vazio.append(city_clima_date)
            else:
                print(f"Falhou!: {city_id}")  # Exibe mensagem se a solicitação falhar

        return pd.DataFrame(df_vazio)

    # Cria e armazena informações básicas da cidade em um arquivo Parquet
    def bronze_city_information(self, df, ref_month, ref_day):
        """
        Função para extrair e armazenar informações básicas da cidade em um arquivo Parquet.

        Args:
            df (DataFrame): DataFrame contendo os dados meteorológicos.
            ref_month (int): Mês de referência.
            ref_day (int): Dia de referência.

        Returns:
            DataFrame: DataFrame contendo as informações básicas da cidade.
        """
        df_information = df[['name', 'id']]
        df_sistema = pd.json_normalize(df['sys'])
        df_sys = df_sistema[['country','sunrise','sunset']]
        df_coord = pd.json_normalize(df['coord'])

        df_concat = pd.concat([df_information, df_coord, df_sys, df['timezone']], ignore_index=False, axis=1)
        df_resulted = df_concat.rename(columns={'name': 'city', 'country': 'sigla', 'id':'id_city'})
        df_resultados = df_resulted[['city','lon','lat', 'sigla', 'id_city','sunrise','sunset','timezone']] 

        # Cria o diretório local e salva os dados no formato Parquet
        self.create_local_directory(self.ref_month, self.ref_day)
        df_resultados.to_parquet(os.path.join(self.local_dir, 'city_information.parquet'), index=False)

        return df_resultados

    # Extrai e armazena informações de temperatura em um arquivo Parquet
    def bronze_temperatures_information(self, df, ref_month, ref_day):
        """
        Função para extrair e armazenar informações de temperatura em um arquivo Parquet.

        Args:
            df (DataFrame): DataFrame contendo os dados meteorológicos.
            ref_month (int): Mês de referência.
            ref_day (int): Dia de referência.

        Returns:
            DataFrame: DataFrame contendo as informações de temperatura.
        """
        df_city_information = self.bronze_city_information(df, ref_month, ref_day)

        df_main = pd.json_normalize(df['main'])
        df_weather = pd.json_normalize(df['weather'])
        df_weather = pd.json_normalize(df_weather[0])

        df_resultados = pd.concat([df_city_information['id_city'], df_main[['temp','feels_like','temp_min', 'temp_max','pressure']], df_weather['id']], ignore_index=False, axis=1) 
        
        # Cria o diretório local e salva os dados no formato Parquet
        self.create_local_directory(self.ref_month, self.ref_day)
        df_resultados.to_parquet(os.path.join(self.local_dir, 'temperatures_information.parquet'), index=False)

        return df_resultados

    # Extrai e armazena informações meteorológicas do dia em um arquivo Parquet
    def bronze_weather_of_the_day(self, df, ref_month, ref_day):
        """
        Função para extrair e armazenar informações meteorológicas do dia em um arquivo Parquet.

        Args:
            df (DataFrame): DataFrame contendo os dados meteorológicos.
            ref_month (int): Mês de referência.
            ref_day (int): Dia de referência.

        Returns:
            DataFrame: DataFrame contendo as informações meteorológicas do dia.
        """
        df_weather = pd.json_normalize(df['weather'])
        df_weather = pd.json_normalize(df_weather[0])

        # Verifica se a coluna 'rain' está presente no DataFrame
        if 'rain' in df.columns:
            df_rain = pd.json_normalize(df['rain'])
            df_rain = df_rain.rename(columns={'1h': 'rain'})
            # Substitui valores nulos por 0 na coluna 'rain'
            df_rain['rain'].fillna(0, inplace=True)
        else:
            # Se a coluna 'rain' não existe, cria uma coluna 'rain' preenchida com 0
            df_rain = pd.DataFrame({'rain': [0] * len(df)})
            
        df_resultados = pd.concat([df_weather[['id','main','description']], df['dt'], df_rain], axis=1)

        # Cria o diretório local e salva os dados no formato Parquet
        self.create_local_directory(self.ref_month, self.ref_day)
        df_resultados.to_parquet(os.path.join(self.local_dir, 'weather_of_day.parquet'), index=False)
        
        return df_resultados

    # Cria e armazena informações de vento em um arquivo Parquet
    def bronze_wind_information(self, df, ref_month, ref_day):
        """
        Função para extrair e armazenar informações de vento em um arquivo Parquet.

        Args:
            df (DataFrame): DataFrame contendo os dados meteorológicos.
            ref_month (int): Mês de referência.
            ref_day (int): Dia de referência.

        Returns:
            DataFrame: DataFrame contendo as informações de vento.
        """
        # Normaliza os dados de vento para um DataFrame
        df_wind = pd.json_normalize(df['wind'])
        
        # Combina os dados de vento com os IDs das cidades
        df_concat = pd.concat([df_wind, df['id']], ignore_index=False, axis=1)
        df_resultados = df_concat.rename(columns={'id':'id_city'})

        # Cria o diretório local e salva os dados no formato Parquet
        self.create_local_directory(self.ref_month, self.ref_day)
        df_resultados.to_parquet(os.path.join(self.local_dir, 'wind_information.parquet'), index=False)
        
        return df_resultados

    def connect_databases():
        """
        Função para estabelecer a conexão com o banco de dados.

        Esta função cria uma instância da classe DatabaseOps e estabelece uma conexão com o banco de dados.

        Returns:
            None
        """
        global database, database_connection

        # Cria uma instância da classe DatabaseOps
        database = DatabaseOps()

        # Estabelece uma conexão com o banco de dados
        database_connection = database.connect_db()

    # Chamada da função para estabelecer a conexão com o banco de dados
    connect_databases()

    def insert_database(self, df, schema, tabela):
        """
        Função para inserir os dados de um DataFrame em uma tabela do banco de dados.

        Args:
            df (DataFrame): DataFrame contendo os dados a serem inseridos.
            schema (str): Nome do esquema onde a tabela está localizada.
            tabela (str): Nome da tabela onde os dados serão inseridos.

        Returns:
            bool: True se a inserção for bem-sucedida, False caso contrário.
        """
        try:
            # Inserir os dados no banco de dados
            database.insert(dataframe=df, schema=schema, table=tabela, if_exists='append')
            return True  # Retornar True se a inserção for bem-sucedida
        except Exception as e:
            # Se ocorrer um erro durante a inserção, imprimir mensagem de erro e retornar False
            print(f"Erro durante a inserção no banco de dados: {e}")
            return False 
    
    # Executa o pipeline completo
    def pipeline(self):
        """
        Executa o pipeline completo de processamento e inserção de dados.

        Retorna:
            None: Se o pipeline for executado com sucesso.
            bool: False se ocorrer algum erro durante o processo.
        """
        try:
            # Carrega a lista de cidades
            br_cidades = self.load_city_list()

            # Coleta dados meteorológicos para as cidades selecionadas
            weather_data = self.fetch_weather_data(br_cidades)

            # Extrai e insere informações básicas da cidade no banco de dados
            city_info = self.bronze_city_information(weather_data, self.ref_month, self.ref_day)
            self.insert_database(city_info, 'bronze', 'city_information')

            # Extrai e insere informações de temperatura no banco de dados
            temperatures_info = self.bronze_temperatures_information(weather_data, self.ref_month, self.ref_day)
            self.insert_database(temperatures_info, 'bronze', 'temperatures_information')

            # Extrai e insere informações meteorológicas do dia no banco de dados
            weather_info = self.bronze_weather_of_the_day(weather_data, self.ref_month, self.ref_day)
            self.insert_database(weather_info, 'bronze', 'weather_of_the_day')

            # Extrai e insere informações de vento no banco de dados
            wind_info = self.bronze_wind_information(weather_data, self.ref_month, self.ref_day)
            self.insert_database(wind_info, 'bronze', 'wind_information')

            return None  # Retorna None se o pipeline for executado com sucesso

        except Exception as e:
            # Se ocorrer um erro durante o pipeline, imprimir mensagem de erro e retornar False
            print(f"Erro durante a execução do pipeline: {e}")
            return False 
