# 🚀 Ingestão de Dados - Zebrinha Azul

Este projeto faz a ingestão de dados de clima e trânsito, processando-os em duas camadas: Bronze e Silver.

# 🧠 Contexto

A Zebrinha Azul é uma startup inovadora que se destaca no mercado por sua expertise em lidar com dados de clima e tráfego. A empresa fornece soluções avançadas para otimizar operações logísticas e proporcionar relatórios para clientes de diversos setores. A atual missão é desenvolver um sistema robusto e escalável para integrar, processar e analisar os dados de clima e tráfego que a Zebrinha Azul coleta.

## 📘 Tabelas

Os scripts desenvolvidos entregam as seguintes tabelas:

| Schema | Tabela | Descrição |
|--------|--------|-----------|
| bronze | city_information | Informações principais de cada cidade, extraídas da API |
| bronze | temperatures_information | Informações sobre temperatura de cada cidade, extraídas da API |
| bronze | weather_of_the_day | Informações sobre o clima do dia, extraídas da API |
| bronze | wind_information | Informações sobre o vento de cada cidade, extraídas da API |
| bronze | traffic_direction | Informações sobre a direção do trânsito, contendo colunas como **star_address**, **end_address**, **duration_hours**, **id_city_origem** e **id_city_destino** |
| silver | city_information | Dados tratados e formatados corretamente, como **horas** e **ref** |
| silver | temperatures_information | Dados tratados e formatados corretamente, como **temp_celsius**, **temp_fahrenheit** e **dt_ingestao** |
| silver | weather_of_the_day | Dados tratados e formatados corretamente, como **data** |
| silver | wind_information | Dados tratados e formatados corretamente, como **km/h** e **mph** |

### Camada Bronze

A camada Bronze é a inicial de ingestão de dados, onde todas as informações das APIs são armazenadas com pouca ou nenhuma transformação, mantendo valores brutos.

### Camada Silver

Na camada Silver, os dados são limpos, normalizados e transformados para uma estrutura mais compreensível. Colunas adicionais são incluídas para auxiliar o time técnico e fornecer valor para áreas de negócio. Esta camada permite consultas mais eficientes para análise de dados.

## ⚙️ Como Rodar o Pipeline

Todo o processo é executado em um único script. Abaixo está a descrição da estrutura de pastas do pipeline de ingestão de dados:

- **src/features**: Contém todos os scripts para gerar tabelas nas camadas Bronze e Silver.
- **src/pipeline.py**: Executa o pipeline e faz a ingestão de dados no SQL Server.
- **src/utils**: Diretório contendo scripts auxiliares para o desenvolvimento do código.

### Executando o Pipeline

Para rodar o pipeline, execute o seguinte comando:

```bash
python pipeline.py
```

### Passando Argumento de Tamanho Amostral

Na hora de executar o pipeline, passamos o argumento `tamanho_amostral`, que define a quantidade de dados que queremos extrair da API de cidades. Como este é um teste, escolhemos 5 amostras. Esse parâmetro permite limitar o número de cidades processadas, facilitando testes e depuração do código.

### Configurações

#### Criando Ambiente Virtual

```bash
python -m venv venv

# Linux
source venv/bin/activate

# Windows
.\venv\Scripts\activate
```

#### Variáveis de Ambiente

Crie um arquivo `.env` e adicione as seguintes informações:

```env
API_TRANSITO_KEY="SUA_CHAVE"
API_CLIMA_KEY="SUA_CHAVE"
SERVER="SERVIDOR_BANCO"
```

## Diagrama Relacional

Para acessar o diagrama relacional, veja a imagem abaixo:

<img src="./src/images/diagrama.png" alt="Diagrama Relacional">


---

Desenvolvedor: Arthur Fillipe Oliveira Rocha