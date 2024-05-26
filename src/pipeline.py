from features.feat_bronze_clima import ClimateData
from features.feat_bronze_transito import TrafficData
from features.feat_silver_clima import IntegracaoSilver
import time

if __name__ == '__main__':
    start_time = time.time()
    try:
        print('[insercao][schema: bronze][dados: clima]')
        ClimateData(json_cities='./data/city_list.json', tamanho_amostral=5).pipeline()
        print('sucesso!\n')
    except Exception as e:
        print(f'[erro][schema: bronze][dados: clima]\n{e}')

    try:
        print('[insercao][schema: bronze][dados: transito]')
        TrafficData().pipeline()
        print('sucesso!\n')
    except Exception as e:
        print(f'[erro][schema: bronze][dados: transito]\n{e}')

    try:
        print('[insercao][schema: silver][dados: clima]')
        IntegracaoSilver().pipeline()
        print('sucesso!\n')
    except Exception as e:
        print(f'[erro][schema: silver][dados: clima]\n{e}')

    end_time = time.time()

    # Calculate the execution time
    execution_time_seconds = end_time - start_time
    hours = int(execution_time_seconds // 3600)
    minutes = int((execution_time_seconds % 3600) // 60)
    seconds = int(execution_time_seconds % 60)

    print(f"Tempo de execução: {hours} horas, {minutes} minutos e {seconds} segundos.")
