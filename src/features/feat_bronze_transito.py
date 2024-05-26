import pandas as pd
import requests
import json
import os
import sys
import pytz
from datetime import datetime, timedelta
from dotenv import load_dotenv

src_dir = os.path.join(os.getcwd().split('src')[0], 'src','utils')
sys.path.insert(0, src_dir)
from utils.database_operations import DatabaseOps



class TrafficData:
    """
    Classe para coleta e processamento de dados de tráfego entre cidades usando a API do Google Maps.
    """

    def __init__(self, insert_method: str='append'):
        """
        Inicializa a instância da classe TrafficData com data atual e outras variáveis necessárias para a integração dos dados.
        """
        self.today = datetime.now().date()  # Define a data atual
        self.ref_month = self.today.month  # Define o mês de referência
        self.ref_day = self.today.day  # Define o dia de referência
        self.directions_results = []  # Lista para armazenar os resultados das direções
        self.cidades_origem = []  # Lista para armazenar as cidades de origem
        self.cidades_destino = []  # Lista para armazenar as cidades de destino
        self.df_trafego = pd.DataFrame()  # DataFrame para armazenar os dados de tráfego
        self.insert_method = insert_method # metodo de inserção no banco de dados


    # Função para obter dados da API de Directions
    def get_directions_data(self, origin, destination):
        """
        Obtém dados de direção da API do Google Maps entre dois pontos geográficos.

        Parâmetros:
        origin (str): Coordenadas de origem no formato 'latitude,longitude'.
        destination (str): Coordenadas de destino no formato 'latitude,longitude'.

        Retorna:
        dict: Dados JSON com informações de direção das cidades.
        """
        API_TRANSITO_KEY = os.getenv('API_TRANSITO_KEY')  # Chave da API de tráfego

        # Monta a URL da API
        url = f"https://maps.googleapis.com/maps/api/directions/json?origin={origin}&destination={destination}&key={API_TRANSITO_KEY}"
        response = requests.get(url)  # Faz a requisição GET

        if response.status_code == 200:  # Verifica se a requisição foi bem-sucedida
            return response.json()  # Retorna os dados JSON
        else:
            return None  # Retorna None em caso de falha na requisição

    def collect_directions(self):
        """
        Coleta dados de direção para todas as combinações de cidades.
        """
        # Carrega os dados de cidades da camada Bronze
        BRONZE_DIR = os.path.join(fr'..\data\bronze\{self.ref_month}\{self.ref_day}')
        dir_information = pd.read_parquet(os.path.join(BRONZE_DIR, 'city_information.parquet'), columns=['city','id_city','lon','lat'])

        # Loop para todas as combinações de cidades
        for i in range(len(dir_information)):
            for j in range(i + 1, len(dir_information)):
                origin = f"{dir_information.iloc[i]['lat']},{dir_information.iloc[i]['lon']}"  # Coordenadas de origem
                destination = f"{dir_information.iloc[j]['lat']},{dir_information.iloc[j]['lon']}"  # Coordenadas de destino
                directions_data = self.get_directions_data(origin, destination)  # Obtém os dados de direção
                
                if directions_data:  # Verifica se os dados foram obtidos com sucesso
                    # Adiciona os resultados às listas
                    self.directions_results.append({'directions': directions_data})
                    self.cidades_origem.append(dir_information.iloc[i]['id_city'])
                    self.cidades_destino.append(dir_information.iloc[j]['id_city'])
                else:
                    # Imprime uma mensagem de erro se os dados não puderem ser obtidos
                    print(f"Não foi possível obter os dados de direção para o par {dir_information.iloc[i]['city']} -> {dir_information.iloc[j]['city']}.")

    def process_directions(self):
        """
        Processa os dados de direção coletados e os organiza em um DataFrame.
        """
        direction_information = pd.DataFrame(self.directions_results)  # DataFrame com os resultados de direção
        distances_results = []  # Lista para armazenar as distâncias

        # Loop sobre os resultados de direção
        for directions_data in direction_information['directions']:
            routes = directions_data.get('routes', [])  # Obtém as rotas
            for route in routes:
                legs = route.get('legs', [])  # Obtém as pernas da rota
                for leg in legs:
                    distance = leg.get('distance', {}).get('text')  # Obtém a distância
                    duration = leg.get('duration', {}).get('text')  # Obtém a duração
                    start_address = leg.get('start_address')  # Obtém o endereço de partida
                    end_address = leg.get('end_address')  # Obtém o endereço de chegada
                    distances_results.append({  # Adiciona os resultados à lista
                        'start_address': start_address,
                        'end_address': end_address,
                        'distance': distance,
                        'duration': duration
                    })

        # Cria o DataFrame de tráfego a partir dos resultados de distância
        self.df_trafego = pd.json_normalize(distances_results)
        self.df_trafego['id_city_origem'] = self.cidades_origem  # Adiciona a cidade de origem
        self.df_trafego['id_city_destino'] = self.cidades_destino  # Adiciona a cidade de destino
        self.df_trafego['dt_ingestao'] = self.today  # Adiciona a data de ingestão

    def connect_databases():
        """
        Método para conectar ao banco de dados.
        """
        global database, database_connection

        database = DatabaseOps()
        database_connection = database.connect_db()
    
    connect_databases()

    def insert_database(self, df, schmea, tabela):
        """
        Método para inserir dados no banco de dados.

        Parâmetros:
        df (DataFrame): DataFrame contendo os dados a serem inseridos.
        schema (str): Nome do schema no banco de dados.
        table (str): Nome da tabela no banco de dados.

        Retorna:
        bool: True se a inserção for bem-sucedida, False caso contrário.
        """
        try:
            database.insert(dataframe=df, schema=schmea, table=tabela, if_exists=self.insert_method)
            return True  # Retorna True se a inserção for bem-sucedida
        except Exception as e:
            print(f"[erro][feat_bronze_transito][def: insert_database]\nErro durante a inserção no banco de dados: {e}")
            return False  # Retorna False em caso de erro na inserção

    def pipeline(self):
        """
        Executa a coleta e o processamento dos dados de tráfego.

        Retorna:
        DataFrame: Dados de tráfego processados.
        """
        try:
            self.collect_directions()  # Coleta os dados de direção
            self.process_directions()  # Processa os dados de direção
            self.insert_database(self.df_trafego, 'bronze', 'traffic_direction')  # Insere os dados no banco de dados

            return self.df_trafego  # Retorna os dados de tráfego processados

        except Exception as e:
            print(f"[erro][feat_bronze_transito][def: pipeline]\nErro durante a inserção no banco de dados: {e}")
            return False  # Retorna False em caso de erro durante o pipeline
