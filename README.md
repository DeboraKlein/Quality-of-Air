# Quality-of-Air

import pandas as pd
import numpy as np
from datetime import datetime

# Definição dos padrões de qualidade do ar
PADROES = {
    'PM2.5': {'CONAMA_24h': 25, 'CONAMA_anual': 10, 'OMS_24h': 15, 'OMS_anual': 5},
    'PM10': {'CONAMA_24h': 60, 'CONAMA_anual': 30, 'OMS_24h': 45, 'OMS_anual': 15},
    'NO2': {'CONAMA_1h': 240, 'CONAMA_anual': 40, 'OMS_24h': 25, 'OMS_anual': 10},
    'O3': {'CONAMA_8h': 160, 'OMS_8h': 100},
    'SO2': {'CONAMA_24h': 125, 'CONAMA_1h': 350, 'OMS_24h': 40},
    'CO': {'CONAMA_8h': 10, 'OMS_24h': 4}  # mg/m³
}

# Parâmetros do IQAr conforme documento do MMA
CLASSES_IQAR = {
    'Boa': (0, 40),
    'Regular': (41, 80),
    'Inadequada': (81, 120),
    'Má': (121, 200),
    'Péssima': (201, 300),
    'Crítica': (301, float('inf'))
}

PARAMETROS_IQAR = {
    'PM10': {
        'Boa': (0, 50),
        'Regular': (51, 100),
        'Inadequada': (101, 150),
        'Má': (151, 250),
        'Péssima': (251, 350),
        'Crítica': (351, float('inf'))
    },
    'PM2.5': {
        'Boa': (0, 25),
        'Regular': (26, 50),
        'Inadequada': (51, 75),
        'Má': (76, 125),
        'Péssima': (126, 175),
        'Crítica': (176, float('inf'))
    },
    'O3': {
        'Boa': (0, 80),
        'Regular': (81, 160),
        'Inadequada': (161, 200),
        'Má': (201, 300),
        'Péssima': (301, 400),
        'Crítica': (401, float('inf'))
    },
    'CO': {  # em ppm
        'Boa': (0, 4.4),
        'Regular': (4.5, 9.4),
        'Inadequada': (9.5, 12.4),
        'Má': (12.5, 15.4),
        'Péssima': (15.5, 30.4),
        'Crítica': (30.5, float('inf'))
    },
    'NO2': {
        'Boa': (0, 100),
        'Regular': (101, 200),
        'Inadequada': (201, 1130),
        'Má': (1131, 2260),
        'Péssima': (2261, 3000),
        'Crítica': (3001, float('inf'))
    },
    'SO2': {
        'Boa': (0, 20),
        'Regular': (21, 40),
        'Inadequada': (41, 365),
        'Má': (366, 800),
        'Péssima': (801, 1600),
        'Crítica': (1601, float('inf'))
    }
}

def load_data(file_path):
    """Carrega e prepara os dados."""
    df = pd.read_csv(file_path, sep=',')
    df['Data_Hora'] = pd.to_datetime(df['Data'] + ' ' + df['Hora'])
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
    df = df.dropna(subset=['Valor', 'Data_Hora'])
    return df

def calcular_iqar(valor, poluente):
    """Calcula o IQAr para um poluente específico."""
    if pd.isna(valor) or poluente not in PARAMETROS_IQAR:
        return None
    
    try:
        if poluente == 'CO':
            valor = valor * 0.873  # Conversão mg/m³ para ppm
        
        faixas = PARAMETROS_IQAR[poluente]
        
        for classe, (minimo, maximo) in faixas.items():
            if minimo <= valor <= maximo:
                iqar_min, iqar_max = CLASSES_IQAR[classe]
                proporcao = (valor - minimo) / (maximo - minimo)
                iqar = iqar_min + (iqar_max - iqar_min) * proporcao
                return {
                    'IQAr': round(float(iqar)),  # Convertendo explicitamente para float antes de arredondar
                    'Classe': classe,
                    'Poluente': poluente,
                    'Valor': valor
                }
    except (ValueError, TypeError, ZeroDivisionError):
        return None
    
    return None

def calcular_iqar_global(df):
    """Calcula o IQAr global para cada registro."""
    resultados = []
    
    # Usando 'h' em vez de 'H' para evitar o warning de depreciação
    grupos = df.groupby(pd.Grouper(key='Data_Hora', freq='h'))
    
    for data_hora, grupo in grupos:
        iqares = []
        
        for poluente in grupo['Poluente'].unique():
            valor_max = grupo[grupo['Poluente'] == poluente]['Valor'].max()
            if not pd.isna(valor_max):
                iqar = calcular_iqar(valor_max, poluente)
                if iqar:
                    iqares.append(iqar)
        
        if iqares:
            iqar_global = max(iqares, key=lambda x: x['IQAr'])
            resultados.append({
                'Data_Hora': data_hora,
                'IQAr_Global': iqar_global['IQAr'],
                'Classe_Global': iqar_global['Classe'],
                'Poluente_Critico': iqar_global['Poluente'],
                'Valor_Poluente_Critico': iqar_global['Valor']
            })
    
    return pd.DataFrame(resultados)

def analyze_pollutants(df):
    """Realiza todas as análises de poluentes."""
    resultados = {}
    
    for poluente in df['Poluente'].unique():
        pol_df = df[df['Poluente'] == poluente].copy()
        
        if 'CONAMA_24h' in PADROES.get(poluente, {}):
            diario = pol_df.resample('D', on='Data_Hora')['Valor'].mean()
            resultados[f'{poluente}_CONAMA_24h_violacoes'] = sum(diario > PADROES[poluente]['CONAMA_24h'])
            resultados[f'{poluente}_OMS_24h_violacoes'] = sum(diario > PADROES[poluente]['OMS_24h'])
            resultados[f'{poluente}_max_24h'] = diario.max()
        
        if 'CONAMA_anual' in PADROES.get(poluente, {}):
            anual = pol_df.resample('YE', on='Data_Hora')['Valor'].mean()
            resultados[f'{poluente}_CONAMA_anual_violacao'] = bool(anual.iloc[-1] > PADROES[poluente]['CONAMA_anual'])
            resultados[f'{poluente}_OMS_anual_violacao'] = bool(anual.iloc[-1] > PADROES[poluente]['OMS_anual'])
            resultados[f'{poluente}_media_anual'] = anual.iloc[-1]
    
    df['Hora'] = df['Data_Hora'].dt.hour
    hora_poluente = df.groupby(['Hora', 'Poluente'])['Valor'].mean().unstack()
    resultados['horario_max_poluente'] = hora_poluente.idxmax().to_dict()
    
    estacao_poluente = df.groupby(['Estação', 'Poluente'])['Valor'].mean().unstack()
    resultados['estacao_max_poluente'] = estacao_poluente.idxmax().to_dict()
    
    dia_poluente = df.groupby([df['Data_Hora'].dt.date, 'Poluente'])['Valor'].mean().unstack()
    resultados['dia_max_poluente'] = dia_poluente.idxmax().to_dict()
    
    mes_poluente = df.groupby([df['Data_Hora'].dt.month, 'Poluente'])['Valor'].mean().unstack()
    resultados['mes_max_poluente'] = mes_poluente.idxmax().to_dict()
    
    if df['Data_Hora'].dt.year.nunique() > 1:
        ano_poluente = df.groupby([df['Data_Hora'].dt.year, 'Poluente'])['Valor'].mean().unstack()
        resultados['ano_max_poluente'] = ano_poluente.idxmax().to_dict()
    
    return resultados

def generate_complete_report(df):
    """Gera um relatório completo incluindo IQAr."""
    report = {
        'analise_geral': analyze_pollutants(df),
        'estatisticas_estacoes': df.groupby('Estação')['Valor'].agg(['mean', 'max', 'count']).to_dict(),
        'estatisticas_poluentes': df.groupby('Poluente')['Valor'].agg(['mean', 'max', 'count']).to_dict()
    }
    
    try:
        iqar_df = calcular_iqar_global(df)
        report['iqar_por_hora'] = iqar_df.to_dict('records')
        
        if not iqar_df.empty:
            report['estatisticas_iqar'] = {
                'media_iqar': float(iqar_df['IQAr_Global'].mean()),
                'max_iqar': float(iqar_df['IQAr_Global'].max()),
                'classe_mais_frequente': iqar_df['Classe_Global'].mode()[0] if not iqar_df['Classe_Global'].empty else None,
                'poluente_mais_critico': iqar_df['Poluente_Critico'].mode()[0] if not iqar_df['Poluente_Critico'].empty else None
            }
    except Exception as e:
        print(f"Erro ao calcular IQAr: {e}")
        report['iqar_por_hora'] = []
        report['estatisticas_iqar'] = {}
    
    df['Data'] = df['Data_Hora'].dt.date
    df['Mes'] = df['Data_Hora'].dt.month
    df['Ano'] = df['Data_Hora'].dt.year
    
    max_values = df.loc[df.groupby('Poluente')['Valor'].idxmax()]
    report['registros_maximos'] = max_values.to_dict('records')
    
    return report

def save_report(report, filename):
    """Salva o relatório em um arquivo CSV formatado."""
    flat_report = {}
    for category, data in report.items():
        if isinstance(data, dict):
            for key, value in data.items():
                flat_report[f"{category}_{key}"] = value
        else:
            flat_report[category] = data
    
    pd.DataFrame.from_dict(flat_report, orient='index').to_csv(filename, sep=';')
    print(f"Relatório completo salvo em {filename}")

if __name__ == "__main__":
    dados = load_data('2022ES1.csv')
    dados.loc[dados['Poluente'] == 'CO', 'Valor'] = dados[dados['Poluente'] == 'CO']['Valor'] * 1000
    
    relatorio_completo = generate_complete_report(dados)
    save_report(relatorio_completo, 'relatorio_completo_iqar.csv')
    
    print("\nResumo da Análise:")
    print("="*50)
    for poluente, stats in relatorio_completo['analise_geral'].items():
        if any(x in poluente for x in ['violacoes', 'max']):
            print(f"{poluente}: {stats}")
    
    print("\nEstações com maiores concentrações:")
    if 'estacao_max_poluente' in relatorio_completo['analise_geral']:
        for poluente, estacao in relatorio_completo['analise_geral']['estacao_max_poluente'].items():
            print(f"{poluente}: {estacao}")
    
    print("\nResumo do IQAr:")
    print("="*50)
    if 'estatisticas_iqar' in relatorio_completo:
        print(f"Maior IQAr registrado: {relatorio_completo['estatisticas_iqar'].get('max_iqar', 'N/A')}")
        print(f"Classe mais frequente: {relatorio_completo['estatisticas_iqar'].get('classe_mais_frequente', 'N/A')}")
        print(f"Poluente mais crítico: {relatorio_completo['estatisticas_iqar'].get('poluente_mais_critico', 'N/A')}")
