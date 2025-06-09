# Quality-of-Air

import pandas as pd
import numpy as np
import os
from datetime import datetime

# [Seção 1: Configurações e Parâmetros] 
PADROES = {
    'O3': {'CONAMA_1h': 160, 'OMS_8h': 100},
    'PM2.5': {'CONAMA_24h': 25, 'OMS_24h': 15},
    'PM10': {'CONAMA_24h': 50, 'OMS_24h': 45},
    'CO': {'CONAMA_8h': 9, 'OMS_8h': 6},
    'NO2': {'CONAMA_1h': 320, 'OMS_1h': 200}
}

CLASSES_IQAR = [
    (0, 50, 'Bom'),
    (51, 100, 'Regular'),
    (101, 150, 'Inadequado'),
    (151, 200, 'Ruim'),
    (201, 300, 'Muito Ruim'),
    (301, float('inf'), 'Péssimo')
]

def calcular_iqar_global(df):
    """Calcula o Índice de Qualidade do Ar (IQAr) para cada registro"""
    # Versão corrigida usando apply
    def classificar_iqar(valor):
        for min_val, max_val, classe in CLASSES_IQAR:
            if min_val <= valor <= max_val:
                return classe
        return 'Desconhecido'
    
    df['IQAr'] = df['Valor'].apply(classificar_iqar)
    return df[['Data_Hora', 'Poluente', 'IQAr', 'Valor', 'Estacao']]

def calcular_violacoes(df):
    """Calcula violações dos padrões de qualidade do ar."""
    violacoes = []
    
    for poluente, padroes in PADROES.items():
        # Filtra dados para o poluente atual
        pol_df = df[df['Poluente'] == poluente]
        
        if pol_df.empty:
            continue
            
        # Calcula médias diárias
        diario = pol_df.resample('D', on='Data_Hora')['Valor'].mean().reset_index()
        diario['Poluente'] = poluente
        diario['Ano'] = diario['Data_Hora'].dt.year
        
        # Verifica violações para cada padrão
        if 'CONAMA_1h' in padroes:
            diario['Violacao_CONAMA_1h'] = diario['Valor'] > padroes['CONAMA_1h']
        if 'CONAMA_24h' in padroes:
            diario['Violacao_CONAMA_24h'] = diario['Valor'] > padroes['CONAMA_24h']
        if 'OMS_8h' in padroes:
            diario['Violacao_OMS_8h'] = diario['Valor'] > padroes['OMS_8h']
        if 'OMS_24h' in padroes:
            diario['Violacao_OMS_24h'] = diario['Valor'] > padroes['OMS_24h']
        
        violacoes.append(diario)
    
    return pd.concat(violacoes) if violacoes else pd.DataFrame()

def process_annual_data(file_path, year):
    """Processa um arquivo anual e retorna tabelas resumidas."""
    print(f"Processando {year}...")
    
    try:
        # Carrega e prepara os dados
        df = pd.read_csv(file_path, sep=',', encoding='utf-8-sig')
        
        # Verifica colunas disponíveis
        print(f"Colunas encontradas: {df.columns.tolist()}")
        
        # Conversão robusta de data/hora
        try:
            df['Data_Hora'] = pd.to_datetime(df['Data'] + ' ' + df['Hora'], format='%d/%m/%Y %H:%M')
        except:
            try:
                df['Data_Hora'] = pd.to_datetime(df['Data'] + ' ' + df['Hora'], format='%Y-%m-%d %H:%M:%S')
            except:
                df['Data_Hora'] = pd.to_datetime(df['Data'] + ' ' + df['Hora'], format='mixed')
        
        # Converte valor para numérico
        df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
        
        # Remove registros inválidos
        df = df.dropna(subset=['Valor', 'Data_Hora'])
        
        # Conversão de unidades (CO de mg/m³ para µg/m³)
        if 'Unidade' in df.columns:
            df.loc[(df['Poluente'] == 'CO') & (df['Unidade'] == 'mg/m³'), 'Valor'] *= 1000
        
        # Adiciona colunas temporais
        df['Ano'] = year
        df['Mes'] = df['Data_Hora'].dt.month
        df['Dia'] = df['Data_Hora'].dt.day
        
        # Cálculo do IQAr horário
        iqar_horario = calcular_iqar_global(df)
        iqar_horario['Ano'] = year
        
        # Resumos por período
        resumo_mensal = df.groupby(['Ano', 'Mes', 'Poluente']).agg(
            Media=('Valor', 'mean'),
            Maximo=('Valor', 'max'),
            Minimo=('Valor', 'min'),
            Contagem=('Valor', 'count')
        ).reset_index()
        
        resumo_estacao = df.groupby(['Ano', 'Estacao', 'Poluente']).agg(
            Media=('Valor', 'mean'),
            Maximo=('Valor', 'max'),
            Contagem=('Valor', 'count')
        ).reset_index()
        
        violacoes = calcular_violacoes(df)
        
        return {
            'iqar_horario': iqar_horario,
            'resumo_mensal': resumo_mensal,
            'resumo_estacao': resumo_estacao,
            'violacoes': violacoes
        }
    except Exception as e:
        print(f"Erro ao processar {year}: {str(e)}")
        return None

# [Restante do código permanece igual...]
def consolidar_anos(base_folder, anos):
    """Consolida dados de múltiplos anos."""
    tabelas_consolidadas = {
        'iqar_horario': [],
        'resumo_mensal': [],
        'resumo_estacao': [],
        'violacoes': []
    }
    
    for ano in anos:
        file_path = os.path.join(base_folder, f"{ano}ES1.csv")
        print(f"Verificando: {file_path}")  # Debug
        
        if os.path.exists(file_path):
            dados_ano = process_annual_data(file_path, ano)
            if dados_ano:  # Só adiciona se o processamento foi bem-sucedido
                for tabela in tabelas_consolidadas:
                    tabelas_consolidadas[tabela].append(dados_ano[tabela])
        else:
            print(f"⚠️ Arquivo não encontrado: {file_path}")
    
    # Verificação final
    if not any(len(lst) > 0 for lst in tabelas_consolidadas.values()):
        raise ValueError("Nenhum dado foi processado. Verifique os arquivos na pasta.")
    
    return {
        'IQAr_Horario': pd.concat(tabelas_consolidadas['iqar_horario']),
        'Resumo_Mensal': pd.concat(tabelas_consolidadas['resumo_mensal']),
        'Resumo_Estacao': pd.concat(tabelas_consolidadas['resumo_estacao']),
        'Violacoes': pd.concat(tabelas_consolidadas['violacoes'])
    }

if __name__ == "__main__":
    # Configurações com caminho absoluto
    PASTA_DADOS = r"C:\Users\debor\OneDrive\Github\Qualidade do Ar\ES"
    PASTA_SAIDA = "powerbi_resumo"
    ANOS = range(2015, 2023)  # 2015-2022
    
    # Processamento
    try:
        print("Iniciando processamento...")
        tabelas_consolidadas = consolidar_anos(PASTA_DADOS, ANOS)
        save_for_powerbi(tabelas_consolidadas, PASTA_SAIDA)
        print("✅ Processamento concluído com sucesso!")
    except Exception as e:
        print(f"❌ Erro durante o processamento: {str(e)}")
