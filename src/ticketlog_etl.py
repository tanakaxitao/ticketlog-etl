import requests
import psycopg2
from datetime import datetime, timedelta
import certifi
from dotenv import load_dotenv
import os

# Carrega variáveis do arquivo .env
load_dotenv()

# Variáveis do banco de dados e token da API
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

url = "https://srv1.ticketlog.com.br/ticketlog-servicos/ebs/transacaoVeiculo/search"
authorization_token = os.getenv("AUTHORIZATION_TOKEN")

headers = {
    "Authorization": authorization_token,
    "Content-Type": "application/json"
}

codigo_clientes = [
    156474, 158487, 158130, 200020, 153216, 224600, 165265, 235339, 235375, 235376,
    235338, 235335, 153946, 241711, 235378, 237034, 158312, 159439, 212607, 159424
]

def create_table():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cursor = conn.cursor()
        cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS ticketlog;
        CREATE TABLE IF NOT EXISTS ticketlog.transacoes (
            id SERIAL PRIMARY KEY,
            codigo_transacao BIGINT NOT NULL,
            data_transacao TIMESTAMP NOT NULL,
            placa VARCHAR(20) NOT NULL,
            veiculo_fabricante VARCHAR(100),
            veiculo_modelo VARCHAR(100),
            uf VARCHAR(2),
            litros DECIMAL(10, 2),
            valor_transacao DECIMAL(10, 2),
            nome_motorista VARCHAR(100),
            codigo_estabelecimento VARCHAR(20),
            tipo_combustivel VARCHAR(50),
            quilometragem INTEGER,
            grupo_restricao_transacao VARCHAR(100),
            codigo_cliente BIGINT
        );
        """)
        conn.commit()
        print("✅ Tabela 'ticketlog.transacoes' criada/verificada.")
    except Exception as e:
        print(f"⚠️ Erro ao criar ou alterar a tabela: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def carregar_codigos_existentes():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cursor = conn.cursor()
        cursor.execute("SELECT codigo_transacao FROM ticketlog.transacoes")
        codigos = {row[0] for row in cursor.fetchall()}
        print(f"📌 {len(codigos)} transações já existem no banco.")
        return codigos
    except Exception as e:
        print(f"⚠️ Erro ao buscar códigos existentes: {e}")
        return set()
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def insert_transaction(cursor, transacao, codigo_cliente):
    insert_sql = """
        INSERT INTO ticketlog.transacoes (
            codigo_transacao, data_transacao, placa, veiculo_fabricante,
            veiculo_modelo, uf, litros, valor_transacao, nome_motorista,
            codigo_estabelecimento, tipo_combustivel, quilometragem,
            grupo_restricao_transacao, codigo_cliente
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor.execute(insert_sql, (
        transacao['codigoTransacao'],
        transacao['dataTransacao'],
        transacao['placa'],
        transacao.get('veiculoFabricante'),
        transacao.get('veiculoModelo'),
        transacao.get('uf'),
        transacao.get('litros'),
        transacao.get('valorTransacao'),
        transacao.get('nomeMotorista'),
        transacao.get('codigoEstabelecimento'),
        transacao.get('tipoCombustivel'),
        transacao.get('quilometragem'),
        transacao.get('grupoRestricaoTransacao'),
        codigo_cliente
    ))

def fetch_and_save_transactions():
    start_date = datetime(2025, 10, 01)
    end_date = datetime.today()
    current_date = start_date

    create_table()
    codigos_existentes = carregar_codigos_existentes()

    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cursor = conn.cursor()

        while current_date <= end_date:
            data_inicial = current_date.strftime("%Y-%m-%dT00:00:00")
            data_final = current_date.strftime("%Y-%m-%dT23:59:59")

            for codigo_cliente in codigo_clientes:
                payload = {
                    "codigoCliente": codigo_cliente,
                    "codigoTipoCartao": 4,
                    "codigoProduto": 4,
                    "dataTransacaoInicial": data_inicial,
                    "dataTransacaoFinal": data_final,
                    "ordem": "S",
                    "considerarTransacao": "V"
                }

                try:
                    response = requests.post(url, headers=headers, json=payload, timeout=30, verify=certifi.where())
                    if response.status_code == 200:
                        dados = response.json()
                        novas_transacoes = 0

                        if 'transacoes' in dados:
                            for transacao in dados['transacoes']:
                                codigo = transacao['codigoTransacao']
                                if codigo not in codigos_existentes:
                                    insert_transaction(cursor, transacao, codigo_cliente)
                                    codigos_existentes.add(codigo)
                                    novas_transacoes += 1

                            if novas_transacoes:
                                conn.commit()
                                print(f"✅ {novas_transacoes} transações salvas para cliente {codigo_cliente} em {data_inicial}")
                            else:
                                print(f"⏩ Nenhuma nova transação para cliente {codigo_cliente} em {data_inicial}")
                        else:
                            print(f"❌ Nenhuma transação retornada para cliente {codigo_cliente} em {data_inicial}")
                    else:
                        print(f"❌ Erro {response.status_code} na API para cliente {codigo_cliente} em {data_inicial}")
                except Exception as e:
                    print(f"⚠️ Erro ao buscar dados do cliente {codigo_cliente} em {data_inicial}: {e}")

            current_date += timedelta(days=1)

    finally:
        if cursor: cursor.close()
        if conn: conn.close()
