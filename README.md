# Nome Projeto


## 1. Tabelas:

| Schema | Tabela | Descrição |
|---|---|---|
|  bronze | clima   | dados climáticos  |

### 1.1. Camada Bronze

### 1.2. Camada Silver


## 2. Como rodar o pipeline

```bash
python pipeline.py
```

## Configurações

### Criando ambiente virtual
```bash
python -m venv venv

# linux
source activate venv/bin/activate

# windows
.\Scripts\activate
```

### Variáveis de ambiente
Crie um arquivo .env e adicione as informações abaixo:


    API_TRANSITO_KEY = "SUA_CHAVE"
    API_CLIMA_KEY = "SUA_CHAVE"
    SERVER = "SERVIDOR_BANCO"
## Diagrama relacional
Para acessar a imagem, basta 

| Imagem | Diretório|
|--| -- |
| Diagrama Relacional | [diagrama relacional](./imagens/diagrama.png) |
