import pandas as pd
import os

# caminho base do projeto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ----------------------
# FUNÇÃO AUXILIAR
# ----------------------
def faixa_etaria(idade):
    if idade <= 12:
        return 'Criança'
    elif idade <= 18:
        return 'Adolescente'
    elif idade <= 60:
        return 'Adulto'
    else:
        return 'Idoso'


# ----------------------
# EXTRACT
# ----------------------
def extract():
    pacientes = pd.read_csv(os.path.join(BASE_DIR, 'data/raw/pacientes.csv'))
    internacoes = pd.read_csv(os.path.join(BASE_DIR, 'data/raw/internacoes.csv'))
    procedimentos = pd.read_csv(os.path.join(BASE_DIR, 'data/raw/procedimentos.csv'))
    faturamento = pd.read_csv(os.path.join(BASE_DIR, 'data/raw/faturamento.csv'))

    return pacientes, internacoes, procedimentos, faturamento


# ----------------------
# TRANSFORM
# ----------------------
def transform(pacientes, internacoes, procedimentos, faturamento):

    df = faturamento.merge(internacoes, on='id_internacao') \
                    .merge(pacientes, on='id_paciente') \
                    .merge(procedimentos, on='id_procedimento')

    # Conversão de datas
    df['data_entrada'] = pd.to_datetime(df['data_entrada'])
    df['data_saida'] = pd.to_datetime(df['data_saida'])

    # Métricas
    df['tempo_internacao'] = (df['data_saida'] - df['data_entrada']).dt.days
    df['custo_medio_item'] = df['custo_total'] / df['quantidade']

    # Tempo
    df['ano'] = df['data_entrada'].dt.year
    df['mes'] = df['data_entrada'].dt.month
    df['dia'] = df['data_entrada'].dt.day

    # 🔥 NOVA COLUNA (CORREÇÃO DO ERRO)
    df['faixa_etaria'] = df['idade'].apply(faixa_etaria)

    return df


# ----------------------
# LOAD
# ----------------------
def load(df):
    output_path = os.path.join(BASE_DIR, 'data/processed/dados_tratados.csv')
    df.to_csv(output_path, index=False)


# ----------------------
# PIPELINE
# ----------------------
def run_pipeline():
    pacientes, internacoes, procedimentos, faturamento = extract()
    df = transform(pacientes, internacoes, procedimentos, faturamento)
    load(df)


if __name__ == "__main__":
    run_pipeline()