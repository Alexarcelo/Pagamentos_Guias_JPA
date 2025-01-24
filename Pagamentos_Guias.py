import streamlit as st
import pandas as pd
import mysql.connector
import decimal
import numpy as np
from datetime import timedelta, time
from babel.numbers import format_currency
import gspread 
import requests
from google.cloud import secretmanager 
import json
from google.oauth2.service_account import Credentials
from google.oauth2 import service_account

def gerar_df_phoenix(vw_name):
    
    config = {
    'user': 'user_automation_jpa',
    'password': 'luck_jpa_2024',
    'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com',
    'database': 'test_phoenix_joao_pessoa'
    }
    # Conexão as Views
    conexao = mysql.connector.connect(**config)
    cursor = conexao.cursor()

    request_name = f'SELECT * FROM {vw_name}'

    # Script MySql para requests
    cursor.execute(
        request_name
    )
    # Coloca o request em uma variavel
    resultado = cursor.fetchall()
    # Busca apenas o cabecalhos do Banco
    cabecalho = [desc[0] for desc in cursor.description]

    # Fecha a conexão
    cursor.close()
    conexao.close()

    # Coloca em um dataframe e muda o tipo de decimal para float
    df = pd.DataFrame(resultado, columns=cabecalho)
    df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
    return df

def puxar_dados_phoenix():

    st.session_state.df_escalas = gerar_df_phoenix('vw_payment_guide')

    st.session_state.df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Status do Servico']!='CANCELADO') & (~pd.isna(st.session_state.df_escalas['Escala']))].reset_index(drop=True)
    
    st.session_state.df_escalas['Data | Horario Apresentacao'] = pd.to_datetime(st.session_state.df_escalas['Data | Horario Apresentacao'], errors='coerce')
    
    st.session_state.df_escalas['Guia'] = st.session_state.df_escalas['Guia'].fillna('')

def puxar_tarifarios(id_gsheet):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(id_gsheet)
    
    sheet = spreadsheet.worksheet('Tarifario')

    sheet_data = sheet.get_all_values()

    st.session_state.df_tarifario = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

    st.session_state.df_tarifario['Valor'] = pd.to_numeric(st.session_state.df_tarifario['Valor'], errors='coerce')

def agrupar_por_escala():

    df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                             (~st.session_state.df_escalas['Guia'].isin(['', 'SEM GUIA'])) & (~st.session_state.df_escalas['Servico'].str.upper().str.contains('4X4|BUGGY'))]

    df_escalas_group = df_escalas.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Motorista', 'Guia', 'Servico', 'Tipo de Servico', 'Modo'])\
        .agg({'Apoio': transformar_em_string, 'Data Voo': 'first',  'Horario Voo': transformar_em_string, 'Data | Horario Apresentacao': 'min', 'Est. Origem': transformar_em_string}).reset_index()
    
    df_escalas_group['Horario Apresentacao'] = df_escalas_group['Data | Horario Apresentacao'].dt.time

    return df_escalas_group

def gerar_dataframes_base(df_escalas_group):

    df_escalas_pvt_tour_bara = df_escalas_group[(df_escalas_group['Tipo de Servico'].isin(['TOUR', 'TRANSFER'])) & (df_escalas_group['Modo']!='REGULAR') & 
                                                (df_escalas_group['Est. Origem'].str.upper().str.contains('BA´RA HOTEL'))].reset_index(drop=True)
    
    df_escalas_pvt_tour = df_escalas_group[(df_escalas_group['Tipo de Servico'].isin(['TOUR', 'TRANSFER'])) & (df_escalas_group['Modo']!='REGULAR') & 
                                           (~df_escalas_group['Est. Origem'].str.upper().str.contains('BA´RA HOTEL'))].reset_index(drop=True)
    
    df_escalas_reg_tour = df_escalas_group[(df_escalas_group['Tipo de Servico'].isin(['TOUR', 'TRANSFER'])) & (df_escalas_group['Modo']=='REGULAR')].reset_index(drop=True)

    df_escalas_in_out = df_escalas_group[(df_escalas_group['Tipo de Servico'].isin(['IN', 'OUT']))].reset_index(drop=True)
    
    return df_escalas_pvt_tour_bara, df_escalas_pvt_tour, df_escalas_reg_tour, df_escalas_in_out

def gerar_lista_de_servicos_nao_tarifados(df_escalas_pvt_tour_bara, tipo_tarifario, tipo_servico, lista_add_servicos):

    servicos_tarifario_pvt_bara = st.session_state.df_tarifario[st.session_state.df_tarifario['Modo']==tipo_tarifario]['Servico'].unique()

    servicos_escalas_pvt_bara = df_escalas_pvt_tour_bara['Servico'].unique()

    servicos_nao_tarifados_bara = list(set(servicos_escalas_pvt_bara) - set(servicos_tarifario_pvt_bara))

    servicos_nao_tarifados_bara = [[item, tipo_tarifario, tipo_servico] for item in servicos_nao_tarifados_bara]

    lista_add_servicos.extend(servicos_nao_tarifados_bara)

    return lista_add_servicos

def verificar_tarifarios_tt(df_escalas_pvt_tour_bara, df_escalas_pvt_tour, df_escalas_reg_tour, id_gsheet, aba_gsheet):

    # Verificando serviços não tarifados PRIVATIVO BARA

    lista_add_servicos = []

    lista_add_servicos = gerar_lista_de_servicos_nao_tarifados(df_escalas_pvt_tour_bara, 'PRIVATIVO BARA', 'TOUR/TRANSFER', lista_add_servicos)

    # Verificando serviços não tarifados PRIVATIVO

    lista_add_servicos = gerar_lista_de_servicos_nao_tarifados(df_escalas_pvt_tour, 'PRIVATIVO', 'TOUR/TRANSFER', lista_add_servicos)

    # Verificando serviços não tarifados REGULAR

    lista_add_servicos = gerar_lista_de_servicos_nao_tarifados(df_escalas_reg_tour, 'REGULAR', 'TOUR/TRANSFER', lista_add_servicos)

    if len(lista_add_servicos)>0:

        df_itens_faltantes = pd.DataFrame(lista_add_servicos, columns=['Servico', 'Modo', 'Tipo do Servico'])

        st.dataframe(df_itens_faltantes, hide_index=True)

        nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
        credentials = service_account.Credentials.from_service_account_info(nome_credencial)
        scope = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = credentials.with_scopes(scope)
        client = gspread.authorize(credentials)
        
        spreadsheet = client.open_by_key(id_gsheet)

        sheet = spreadsheet.worksheet('Tarifario')
        sheet_data = sheet.get_all_values()
        last_filled_row = len(sheet_data)
        data = df_itens_faltantes.values.tolist()
        start_row = last_filled_row + 1
        start_cell = f"A{start_row}"
        
        sheet.update(start_cell, data)

        st.error('Os serviços acima não estão tarifados. Eles foram inseridos no final da planilha de tarifários. Por favor, tarife os serviços e tente novamente')

        st.stop()

def gerar_dataframe_pagamento(df_servicos, tipo_tarifario):

    df_tarifario = st.session_state.df_tarifario[st.session_state.df_tarifario['Modo']==tipo_tarifario].reset_index(drop=True)

    df_pag = pd.merge(df_servicos, df_tarifario[['Servico', 'Valor']], on = 'Servico', how = 'left')

    return df_pag

def colocar_valores_em_dataframes(df_escalas_pvt_tour_bara, df_escalas_pvt_tour, df_escalas_reg_tour):

    # Colocando valor dos TOURS e TRANSFERS privativos BA´RA
    
    df_pag_guias_pvt_tour_bara = gerar_dataframe_pagamento(df_escalas_pvt_tour_bara, 'PRIVATIVO BARA')

    # Colocando valor dos TOURS e TRANSFERS privativos não BA´RA

    df_pag_guias_pvt_tour = gerar_dataframe_pagamento(df_escalas_pvt_tour, 'PRIVATIVO')

    # Colocando valor dos TOURS e TRANSFERS regulares

    df_pag_guias_reg_tour = gerar_dataframe_pagamento(df_escalas_reg_tour, 'REGULAR')

    return df_pag_guias_pvt_tour_bara, df_pag_guias_pvt_tour, df_pag_guias_reg_tour

def gerar_pag_motoguia(df):

    df['Acréscimo Motoguia'] = np.where(df['Guia'] == df['Motorista'], df['Valor'] * 0.5, 0)

    return df

def calcular_acrescimo_motoguia(df_pag_guias_pvt_tour_bara, df_pag_guias_pvt_tour, df_pag_guias_reg_tour):

    df_pag_guias_pvt_tour_bara = gerar_pag_motoguia(df_pag_guias_pvt_tour_bara)

    df_pag_guias_pvt_tour = gerar_pag_motoguia(df_pag_guias_pvt_tour)

    df_pag_guias_reg_tour = gerar_pag_motoguia(df_pag_guias_reg_tour)

    return df_pag_guias_pvt_tour_bara, df_pag_guias_pvt_tour, df_pag_guias_reg_tour

def ajustar_pag_giuliano_junior_neto_tt(df):

    mask_pag_herbet = (df['Guia'].isin(['HERBET - GUIA'])) & (df['Acréscimo Motoguia']!=0) & ((df['Valor Total']<150) | (pd.isna(df['Valor Total'])))
    
    df.loc[mask_pag_herbet, ['Valor', 'Acréscimo Motoguia', 'Valor Total']] = [150, 0, 150]

    mask_pag_junior_giuliano = (df['Guia'].isin(['GIULIANO - GUIA', 'JUNIOR - GUIA'])) & (df['Acréscimo Motoguia']!=0) & ((df['Valor Total']<270) | (pd.isna(df['Valor Total'])))

    df.loc[mask_pag_junior_giuliano, ['Valor', 'Acréscimo Motoguia', 'Valor Total']] = [270, 0, 270]

    mask_pag_neto = (df['Guia'].isin(['NETO VIANA - GUIA'])) & (df['Acréscimo Motoguia']!=0) & ((df['Valor Total']<350) | (pd.isna(df['Valor Total'])))

    df.loc[mask_pag_neto, ['Valor', 'Acréscimo Motoguia', 'Valor Total']] = [350, 0, 350]
    
    return df

def colunas_voos_mais_tarde_cedo(df_escalas_in_out):

    df_escalas_in_out['Horario Voo Mais Tarde'] = df_escalas_in_out['Horario Voo'].apply(lambda x: max(x.split(', ')))

    df_escalas_in_out['Horario Voo Mais Tarde'] = pd.to_datetime(df_escalas_in_out['Horario Voo Mais Tarde']).dt.time

    df_escalas_in_out['Horario Voo Mais Cedo'] = df_escalas_in_out['Horario Voo'].apply(lambda x: min(x.split(', ')))

    df_escalas_in_out['Horario Voo Mais Cedo'] = pd.to_datetime(df_escalas_in_out['Horario Voo Mais Cedo']).dt.time

    return df_escalas_in_out

def gerar_df_in_out_jpa_interestadual(df_escalas_in_out):

    df_escalas_in_out_jpa_diurno = df_escalas_in_out[(df_escalas_in_out['Servico'].str.upper().str.contains('AEROPORTO JOÃO PESSOA|GUIA BASE')) & 
                                                     (df_escalas_in_out['Diurno / Madrugada']=='DIURNO')].reset_index(drop=True)
    
    df_escalas_in_out_jpa_madrugada = df_escalas_in_out[(df_escalas_in_out['Servico'].str.upper().str.contains('AEROPORTO JOÃO PESSOA|GUIA BASE')) & 
                                                        (df_escalas_in_out['Diurno / Madrugada']=='MADRUGADA')].reset_index(drop=True)
    
    df_escalas_in_out_interestadual = df_escalas_in_out[(~df_escalas_in_out['Servico'].str.upper().str.contains('AEROPORTO JOÃO PESSOA')) & 
                                                        (df_escalas_in_out['Servico'].str.upper().str.contains('AEROPORTO')) & (df_escalas_in_out['Tipo de Servico'].isin(['IN', 'OUT']))]\
                                                            .reset_index(drop=True)
    
    return df_escalas_in_out_jpa_diurno, df_escalas_in_out_jpa_madrugada, df_escalas_in_out_interestadual

def gerar_df_pag_in_out(df_escalas_in_out_jpa_diurno, df_pag_guias_in_out_jpa_madrugada, df_escalas_in_out_interestadual):

    # Gerando dataframe de pagamento de transfers diurnos
    
    df_pag_guias_in_out_jpa_diurno = gerar_dataframe_pagamento(df_escalas_in_out_jpa_diurno, 'TRANSFER DIURNO')

    # Gerando dataframe de pagamento de transfers madrugadas
    
    df_pag_guias_in_out_jpa_madrugada = gerar_dataframe_pagamento(df_escalas_in_out_jpa_madrugada, 'TRANSFER MADRUGADA')

    # Gerando dataframe de pagamento de transfers IN e OUT Interestadual
    
    df_pag_guias_in_out_interestadual = gerar_dataframe_pagamento(df_escalas_in_out_interestadual, 'TRANSFER INTERESTADUAL')

    return df_pag_guias_in_out_jpa_diurno, df_pag_guias_in_out_jpa_madrugada, df_pag_guias_in_out_interestadual

def verificar_juncoes_in_out(df_servicos):

    df_pag_final = pd.DataFrame()

    df_servicos['Desconto por Junção'] = 0

    for guia in df_servicos['Guia'].unique().tolist():
        
        df = df_servicos[df_servicos['Guia']==guia].reset_index(drop=True)
        
        for index in range(1, len(df)):
            
            if (df.at[index, 'Tipo de Servico'] == 'IN' and df.at[index - 1, 'Tipo de Servico'] == 'OUT' and df.at[index, 'Guia'] == df.at[index - 1, 'Guia'] and 
                df.at[index, 'Motorista'] == df.at[index - 1, 'Motorista'] and df.at[index, 'Veiculo'] == df.at[index - 1, 'Veiculo']):
                
                df.at[index, 'Desconto por Junção'] = -df.at[index, 'Valor']-df.at[index, 'Acréscimo Motoguia']
        
        df_pag_final = pd.concat([df_pag_final, df], ignore_index=True)

    return df_pag_final

def criar_coluna_valor_total(df):
        
    if 'Desconto por Junção' in df.columns.tolist():

        df[['Valor', 'Acréscimo Motoguia', 'Desconto por Junção']] = df[['Valor', 'Acréscimo Motoguia', 'Desconto por Junção']].fillna(0)

        df['Valor Total'] = df['Valor'] + df['Acréscimo Motoguia'] + df['Desconto por Junção']

    else:

        df[['Valor', 'Acréscimo Motoguia']] = df[['Valor', 'Acréscimo Motoguia']].fillna(0)

        df['Valor Total'] = df['Valor'] + df['Acréscimo Motoguia']

    return df

def ajustar_pag_giuliano_junior_neto_in_out(df):

    mask_pag_herbet = (df['Guia'].isin(['HERBET - GUIA'])) & (df['Acréscimo Motoguia']!=0) & \
        ((df['Valor Total']<150) | (pd.isna(df['Valor Total'])))
    
    df.loc[mask_pag_herbet, ['Valor', 'Acréscimo Motoguia', 'Valor Total']] = [150, 0, 150]

    mask_pag_junior_giuliano = (df['Guia'].isin(['GIULIANO - GUIA', 'JUNIOR - GUIA'])) & (df['Acréscimo Motoguia']!=0) & ((df['Valor Total']<270) | (pd.isna(df['Valor Total'])))

    df.loc[mask_pag_junior_giuliano, ['Valor', 'Acréscimo Motoguia', 'Valor Total']] = [270, 0, 270]

    mask_pag_neto = (df['Guia'].isin(['NETO VIANA - GUIA'])) & (df['Acréscimo Motoguia']!=0) & ((df['Valor Total']<350) | (pd.isna(df['Valor Total'])))

    df.loc[mask_pag_neto, ['Valor', 'Acréscimo Motoguia', 'Valor Total']] = [350, 0, 350]

    return df

def ajustar_valor_transferistas(df_pag_guias_in_out_final, transferistas):

    mask_transferistas = (df_pag_guias_in_out_final['Guia'].isin(transferistas)) & (df_pag_guias_in_out_final['Valor']<85)

    df_pag_guias_in_out_final.loc[mask_transferistas, ['Valor', 'Acréscimo Motoguia']] = [85, 0]

    return df_pag_guias_in_out_final

def ajustar_colunas_tt_in_out_final(df_pag_guias_tour_total, df_pag_guias_in_out_final):

    df_pag_guias_tour_total['Desconto por Junção'] = 0

    df_pag_guias_tour_total = df_pag_guias_tour_total[['Data da Escala', 'Modo', 'Tipo de Servico', 'Servico', 'Est. Origem', 'Veiculo', 'Motorista', 'Guia', 'Valor', 'Acréscimo Motoguia', 
                                                        'Desconto por Junção', 'Valor Total']]

    df_pag_guias_in_out_final = df_pag_guias_in_out_final[['Data da Escala', 'Modo', 'Tipo de Servico', 'Servico', 'Est. Origem', 'Veiculo', 'Motorista', 'Guia', 'Valor', 'Acréscimo Motoguia', 
                                                            'Desconto por Junção', 'Valor Total']]
    
    df_pag_guias_in_out_final['Est. Origem'] = ''

    return df_pag_guias_tour_total, df_pag_guias_in_out_final

def criar_colunas_escala_veiculo_mot_guia(df_apoios):

    df_apoios[['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio']] = ''

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace('Escala Auxiliar: ', '', regex=False)

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace(' Veículo: ', '', regex=False)

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace(' Motorista: ', '', regex=False)

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace(' Guia: ', '', regex=False)

    df_apoios[['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio']] = \
        df_apoios['Apoio'].str.split(',', expand=True)
    
    return df_apoios

def preencher_colunas_df(df_apoios_group):

    df_apoios_group['Modo']='REGULAR'

    df_apoios_group['Tipo de Servico']='TOUR'

    df_apoios_group['Servico']='APOIO'

    df_apoios_group['Est. Origem']=''

    df_apoios_group[['Valor']]=27

    df_apoios_group[['Acréscimo Motoguia', 'Desconto por Junção', 'Valor Total']]=0

    return df_apoios_group

def definir_html(df_ref):

    html=df_ref.to_html(index=False, escape=False)

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{
                text-align: center;  /* Centraliza o texto */
            }}
            table {{
                margin: 0 auto;  /* Centraliza a tabela */
                border-collapse: collapse;  /* Remove espaço entre as bordas da tabela */
            }}
            th, td {{
                padding: 8px;  /* Adiciona espaço ao redor do texto nas células */
                border: 1px solid black;  /* Adiciona bordas às células */
                text-align: center;
            }}
        </style>
    </head>
    <body>
        {html}
    </body>
    </html>
    """

    return html

def criar_output_html(nome_html, html, guia, soma_servicos):

    with open(nome_html, "w", encoding="utf-8") as file:

        file.write(f'<p style="font-size:40px;">{guia}</p><br><br>')

        file.write(html)

        file.write(f'<br><br><p style="font-size:40px;">O valor total dos serviços é {soma_servicos}</p>')

def puxar_aba_simples(id_gsheet, nome_aba, nome_df):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(id_gsheet)
    
    sheet = spreadsheet.worksheet(nome_aba)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

def verificar_guia_sem_telefone(id_gsheet, guia, lista_guias_com_telefone):

    if not guia in lista_guias_com_telefone:

        lista_guias = []

        lista_guias.append(guia)

        df_itens_faltantes = pd.DataFrame(lista_guias, columns=['Guias'])

        st.dataframe(df_itens_faltantes, hide_index=True)

        nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
        credentials = service_account.Credentials.from_service_account_info(nome_credencial)
        scope = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = credentials.with_scopes(scope)
        client = gspread.authorize(credentials)
        
        spreadsheet = client.open_by_key(id_gsheet)

        sheet = spreadsheet.worksheet('Telefones Guias')
        sheet_data = sheet.get_all_values()
        last_filled_row = len(sheet_data)
        data = df_itens_faltantes.values.tolist()
        start_row = last_filled_row + 1
        start_cell = f"A{start_row}"
        
        sheet.update(start_cell, data)

        st.error(f'O guia {guia} não tem número de telefone cadastrado na planilha. Ele foi inserido no final da lista de guias. Por favor, cadastre o telefone dele e tente novamente')

        st.stop()

    else:

        telefone_guia = st.session_state.df_telefones.loc[st.session_state.df_telefones['Guias']==guia, 'Telefone'].values[0]

    return telefone_guia

def transformar_em_string(apoio):

    return ', '.join(list(set(apoio.dropna())))

def criar_df_apoios():

    df_apoio_filtrado = st.session_state.df_escalas[(~pd.isna(st.session_state.df_escalas['Apoio'])) & (st.session_state.df_escalas['Data da Escala'] >= data_inicial) & 
                                                    (st.session_state.df_escalas['Data da Escala'] <= data_final)].reset_index(drop=True)
    
    df_apoio_filtrado = df_apoio_filtrado.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Motorista', 'Guia', 'Servico', 'Tipo de Servico', 'Modo'])\
        .agg({'Apoio': transformar_em_string, 'Data | Horario Apresentacao': 'min'}).reset_index()

    df_escalas_com_apoio = df_apoio_filtrado[(~df_apoio_filtrado['Apoio'].str.contains(r' \| ', regex=True))].reset_index(drop=True)

    df_escalas_com_apoio = criar_colunas_escala_veiculo_mot_guia(df_escalas_com_apoio)

    df_apoios_group = df_escalas_com_apoio.groupby(['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio'])\
        .agg({'Data da Escala': 'first', 'Data | Horario Apresentacao': 'first'}).reset_index()

    df_apoios_group = df_apoios_group.rename(columns={'Veiculo Apoio': 'Veiculo', 'Motorista Apoio': 'Motorista', 'Guia Apoio': 'Guia', 'Escala Apoio': 'Escala'})

    df_apoios_group = preencher_colunas_df(df_apoios_group)

    df_apoios_group = gerar_pag_motoguia(df_apoios_group)

    df_apoios_group = criar_coluna_valor_total(df_apoios_group)

    df_apoios_group = df_apoios_group.sort_values(by = ['Data da Escala', 'Data | Horario Apresentacao']).reset_index(drop=True)

    df_apoios_group = df_apoios_group[['Data da Escala', 'Modo', 'Tipo de Servico', 'Servico', 'Est. Origem', 'Veiculo', 'Motorista', 'Guia', 'Valor', 'Acréscimo Motoguia', 
                                           'Desconto por Junção', 'Valor Total']]

    df_escalas_com_2_apoios = df_apoio_filtrado[(df_apoio_filtrado['Apoio'].str.contains(r' \| ', regex=True))].reset_index(drop=True)

    df_novo = pd.DataFrame(columns=['Escala', 'Veiculo', 'Motorista', 'Guia', 'Data | Horario Apresentacao', 'Data da Escala'])

    for index in range(len(df_escalas_com_2_apoios)):

        data_escala = df_escalas_com_2_apoios.at[index, 'Data da Escala']

        apoio_nome = df_escalas_com_2_apoios.at[index, 'Apoio']

        data_h_apr = df_escalas_com_2_apoios.at[index, 'Data | Horario Apresentacao']

        lista_apoios = apoio_nome.split(' | ')

        for item in lista_apoios:

            dict_replace = {'Escala Auxiliar: ': '', ' Veículo: ': '', ' Motorista: ': '', ' Guia: ': ''}

            for old, new in dict_replace.items():

                item = item.replace(old, new)
                
            lista_insercao = item.split(',')

            contador = len(df_novo)

            df_novo.at[contador, 'Escala'] = lista_insercao[0]

            df_novo.at[contador, 'Veiculo'] = lista_insercao[1]

            df_novo.at[contador, 'Motorista'] = lista_insercao[2]

            df_novo.at[contador, 'Guia'] = lista_insercao[3]

            df_novo.at[contador, 'Data | Horario Apresentacao'] = data_h_apr

            df_novo.at[contador, 'Data da Escala'] = data_escala

    df_novo = preencher_colunas_df(df_novo)

    df_novo = gerar_pag_motoguia(df_novo)

    df_novo = criar_coluna_valor_total(df_novo)

    df_novo = df_novo.sort_values(by = ['Data da Escala', 'Data | Horario Apresentacao']).reset_index(drop=True)

    df_novo = df_novo[['Data da Escala', 'Modo', 'Tipo de Servico', 'Servico', 'Est. Origem', 'Veiculo', 'Motorista', 'Guia', 'Valor', 'Acréscimo Motoguia', 
                                           'Desconto por Junção', 'Valor Total']]

    df_apoios_group = pd.concat([df_apoios_group, df_novo], ignore_index=True)

    df_apoios_group = df_apoios_group[df_apoios_group['Guia']!='null'].reset_index(drop=True)

    return df_apoios_group

def criar_output_html_geral(nome_html):

    with open(nome_html, "w", encoding="utf-8") as file:

        pass

def inserir_html(nome_html, html, guia, soma_servicos):

    with open(nome_html, "a", encoding="utf-8") as file:

        file.write('<div style="page-break-before: always;"></div>\n')

        file.write(f'<p style="font-size:40px;">{guia}</p><br><br>')

        file.write(html)

        file.write(f'<br><br><p style="font-size:40px;">O valor total dos serviços é {soma_servicos}</p>')

def inserir_dataframe_gsheet(df_itens_faltantes, id_gsheet, nome_aba):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)
    
    spreadsheet = client.open_by_key(id_gsheet)

    sheet = spreadsheet.worksheet(nome_aba)

    sheet.batch_clear(["A2:Z100000"])

    data = df_itens_faltantes.values.tolist()
    sheet.update('A2', data)

st.set_page_config(layout='wide')

st.session_state.id_gsheet = '1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E'

# Puxando dados do Phoenix da 'vw_payment_guide'

if not 'df_escalas' in st.session_state:

    with st.spinner('Puxando dados do Phoenix...'):

        puxar_dados_phoenix()

# Título da página

st.title('Mapa de Pagamento - Guias')

st.divider()

row1 = st.columns(2)

# Objetos pra colher período do mapa

with row1[0]:

    container_datas = st.container(border=True)

    container_datas.subheader('Período')

    data_inicial = container_datas.date_input('Data Inicial', value=None ,format='DD/MM/YYYY', key='data_inicial')

    data_final = container_datas.date_input('Data Final', value=None ,format='DD/MM/YYYY', key='data_final')

# Atualizar Dados Phoenix

with row1[1]:

    row_1_1 = st.columns(2)

    with row_1_1[0]:

        atualizar_phoenix = st.button('Atualizar Dados Phoenix')

        if atualizar_phoenix:

            with st.spinner('Puxando dados do Phoenix...'):

                puxar_dados_phoenix()

st.divider()

# Script pra gerar mapa de pagamento

if data_final and data_inicial:

    # Seleção de transferistas

    with row1[1]:

        lista_guias = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                                  (st.session_state.df_escalas['Guia'] != '')]['Guia'].unique()

        container_transferistas = st.container(border=True)

        transferistas = container_transferistas.multiselect('Selecione os transferistas', sorted(lista_guias), default=None)

    if transferistas:

        row1_2=container_datas.columns(2)

        with row1_2[0]:

            gerar_mapa = st.button('Gerar Mapa de Pagamentos')

        if gerar_mapa:

            with st.spinner('Puxando tarifários do Google Drive...'):

                puxar_tarifarios(st.session_state.id_gsheet)

            with st.spinner('Gerando mapas de pagamentos...'):

                # Agrupando por escala

                df_escalas_group = agrupar_por_escala()

                # Gerando dataframes específicos por tarifa

                df_escalas_pvt_tour_bara, df_escalas_pvt_tour, df_escalas_reg_tour, df_escalas_in_out = gerar_dataframes_base(df_escalas_group)

                # Verificando serviços não tarifados

                lista_add_servicos = verificar_tarifarios_tt(df_escalas_pvt_tour_bara, df_escalas_pvt_tour, df_escalas_reg_tour, st.session_state.id_gsheet, 'Tarifario')

                # Gerando dataframes com valores tarifados

                df_pag_guias_pvt_tour_bara, df_pag_guias_pvt_tour, df_pag_guias_reg_tour = colocar_valores_em_dataframes(df_escalas_pvt_tour_bara, df_escalas_pvt_tour, df_escalas_reg_tour)

                # Calculando acréscimo motoguia

                df_pag_guias_pvt_tour_bara, df_pag_guias_pvt_tour, df_pag_guias_reg_tour = calcular_acrescimo_motoguia(df_pag_guias_pvt_tour_bara, df_pag_guias_pvt_tour, df_pag_guias_reg_tour)

                # Concatenando 'df_pag_guias_reg_tour', 'df_pag_guias_pvt_tour' e 'df_pag_guias_pvt_tour_bara' em um único dataframe

                df_pag_guias_tour_total = pd.concat([df_pag_guias_reg_tour, df_pag_guias_pvt_tour, df_pag_guias_pvt_tour_bara], ignore_index=True)

                # Criando coluna de Valor Total e ordenando por Guia e Data da Escala

                df_pag_guias_tour_total = criar_coluna_valor_total(df_pag_guias_tour_total)

                df_pag_guias_tour_total = df_pag_guias_tour_total.sort_values(by = ['Guia', 'Data da Escala']).reset_index(drop=True)

                # Deixando apenas BA´RA HOTEL na coluna Est. Origem quando o serviço não for regular

                df_pag_guias_tour_total.loc[(df_pag_guias_tour_total['Est. Origem'] != 'BA´RA HOTEL') | (df_pag_guias_tour_total['Modo'] == 'REGULAR'), 'Est. Origem'] = ''
                
                # Ajustando pagamento de Giuliano, Junior e Neto

                df_pag_guias_tour_total = ajustar_pag_giuliano_junior_neto_tt(df_pag_guias_tour_total)

                # Criando colunas com horários de voos mais tarde e mais cedo

                df_escalas_in_out = colunas_voos_mais_tarde_cedo(df_escalas_in_out)

                # Identificando voos diurnos e na madrugada

                df_escalas_in_out['Diurno / Madrugada'] = df_escalas_in_out.apply(lambda row: 'MADRUGADA' if (row['Horario Apresentacao']<=time(4,0)) or 
                                                                                  (row['Horario Voo Mais Tarde']<=time(4)) else 'DIURNO', axis=1)
                
                # Separando diurnos e madrugadas jpa e interestadual

                df_escalas_in_out_jpa_diurno, df_escalas_in_out_jpa_madrugada, df_escalas_in_out_interestadual = gerar_df_in_out_jpa_interestadual(df_escalas_in_out)

                # Gerar dataframes de pagamentos de transfers

                df_pag_guias_in_out_jpa_diurno, df_pag_guias_in_out_jpa_madrugada, df_pag_guias_in_out_interestadual = gerar_df_pag_in_out(df_escalas_in_out_jpa_diurno, df_escalas_in_out_jpa_madrugada, 
                                                                                                                                        df_escalas_in_out_interestadual)
                
                # Concatenando todos os dataframes de pagamento de transfers

                df_pag_guias_in_out = pd.concat([df_pag_guias_in_out_jpa_diurno, df_pag_guias_in_out_jpa_madrugada, df_pag_guias_in_out_interestadual], ignore_index=True)

                # Diminuindo 1 dia dos OUTs da madrugada, mas que tem horário no final do dia anterior

                df_pag_guias_in_out.loc[(df_pag_guias_in_out['Tipo de Servico']=='OUT') & (df_pag_guias_in_out['Diurno / Madrugada']=='MADRUGADA') & 
                                        (df_pag_guias_in_out['Horario Apresentacao']>time(4)), 'Data | Horario Apresentacao'] = \
                                            df_pag_guias_in_out.loc[(df_pag_guias_in_out['Tipo de Servico']=='OUT') & (df_pag_guias_in_out['Diurno / Madrugada']=='MADRUGADA') & 
                                                                    (df_pag_guias_in_out['Horario Apresentacao']>time(4)), 'Data | Horario Apresentacao'] - timedelta(days=1)
                
                # Ordenando por 'Guia', 'Motorista', 'Veiculo', 'Data | Horario Apresentacao'

                df_pag_guias_in_out = df_pag_guias_in_out.sort_values(by = ['Guia', 'Motorista', 'Veiculo', 'Data | Horario Apresentacao']).reset_index(drop=True)

                # Calculando acréscimo motoguia

                df_pag_guias_in_out = gerar_pag_motoguia(df_pag_guias_in_out)

                # Ajustando valor mínimo de transferistas

                df_pag_guias_in_out = ajustar_valor_transferistas(df_pag_guias_in_out, transferistas)

                # Verificando junções de OUTs e INs

                df_pag_guias_in_out_final = verificar_juncoes_in_out(df_pag_guias_in_out)

                # Criando coluna de Valor Total

                df_pag_guias_in_out_final = criar_coluna_valor_total(df_pag_guias_in_out_final)

                # Reordenando por 'Guia', 'Data da Escala'

                df_pag_guias_in_out_final = df_pag_guias_in_out_final.sort_values(by = ['Guia', 'Data da Escala']).reset_index(drop=True)

                # Ajustando pagamentos de Giuliano, Junior e Neto

                df_pag_guias_in_out_final = ajustar_pag_giuliano_junior_neto_in_out(df_pag_guias_in_out_final)

                # Ajustando colunas pra depois concatenar

                df_pag_guias_tour_total, df_pag_guias_in_out_final = ajustar_colunas_tt_in_out_final(df_pag_guias_tour_total, df_pag_guias_in_out_final)

                # Criando Apoios

                df_pag_apoios = criar_df_apoios()

                # Juntando tudo em um dataframe só
                
                df_pag_final = pd.concat([df_pag_guias_tour_total, df_pag_guias_in_out_final, df_pag_apoios], ignore_index=True)

                # Renomeando as colunas

                df_pag_final = df_pag_final.rename(columns={'Tipo de Servico': 'Tipo', 'Servico': 'Serviço', 'Est. Origem': 'Hotel', 'Veiculo': 'Veículo'})

                st.session_state.df_pag_final = df_pag_final

if 'df_pag_final' in st.session_state:

    with row1_2[1]:

        salvar_mapa = st.button('Salvar Mapa de Pagamentos')

    if salvar_mapa:

        with st.spinner('Salvando mapa de pagamentos...'):

            puxar_aba_simples(st.session_state.id_gsheet, 'Histórico de Pagamentos', 'df_historico_pagamentos')

            st.session_state.df_historico_pagamentos['Data da Escala'] = pd.to_datetime(st.session_state.df_historico_pagamentos['Data da Escala'], format='%d/%m/%Y').dt.date

            df_historico_fora_do_periodo = st.session_state.df_historico_pagamentos[~((st.session_state.df_historico_pagamentos['Data da Escala'] >= data_inicial) & 
                                                                                    (st.session_state.df_historico_pagamentos['Data da Escala'] <= data_final))].reset_index(drop=True)
            
            df_insercao = pd.concat([df_historico_fora_do_periodo, st.session_state.df_pag_final], ignore_index=True)

            df_insercao['Data da Escala'] = df_insercao['Data da Escala'].astype(str)

            inserir_dataframe_gsheet(df_insercao, st.session_state.id_gsheet, 'Histórico de Pagamentos')

if 'df_pag_final' in st.session_state:

    st.header('Gerar Mapas')

    row2 = st.columns(2)

    with row2[0]:

        lista_guias = st.session_state.df_pag_final[~(st.session_state.df_pag_final['Guia'].isin(['SEM GUIA', 'NENHUM GUIA', ''])) & (~st.session_state.df_pag_final['Guia'].str.contains('PDV')) & 
                                                    (~st.session_state.df_pag_final['Guia'].str.contains('BASE AEROPORTO')) & 
                                                    (~st.session_state.df_pag_final['Guia'].str.contains('VENDAS ONLINE'))]['Guia'].dropna().unique().tolist()

        guia = st.selectbox('Guia', sorted(lista_guias), index=None)

    if guia:

        row2_1 = st.columns(4)

        df_pag_guia = st.session_state.df_pag_final[st.session_state.df_pag_final['Guia']==guia].sort_values(by=['Data da Escala', 'Veículo', 'Motorista']).reset_index(drop=True)

        df_data_correta = df_pag_guia.reset_index(drop=True)

        df_data_correta['Data da Escala'] = pd.to_datetime(df_data_correta['Data da Escala'])

        df_data_correta['Data da Escala'] = df_data_correta['Data da Escala'].dt.strftime('%d/%m/%Y')

        container_dataframe = st.container()

        container_dataframe.dataframe(df_data_correta, hide_index=True, use_container_width = True)

        with row2_1[0]:

            total_a_pagar = df_pag_guia['Valor Total'].sum()

            st.subheader(f'Valor Total: R${int(total_a_pagar)}')

        df_pag_guia['Data da Escala'] = pd.to_datetime(df_pag_guia['Data da Escala'])

        df_pag_guia['Data da Escala'] = df_pag_guia['Data da Escala'].dt.strftime('%d/%m/%Y')

        soma_servicos = df_pag_guia['Valor Total'].sum()

        soma_servicos = format_currency(soma_servicos, 'BRL', locale='pt_BR')

        for item in ['Valor', 'Acréscimo Motoguia', 'Desconto por Junção', 'Valor Total']:

            df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

        html = definir_html(df_pag_guia)

        nome_html = f'{guia}.html'

        criar_output_html(nome_html, html, guia, soma_servicos)

        with open(nome_html, "r", encoding="utf-8") as file:

            html_content = file.read()

        with row2_1[1]:

            st.download_button(
                label="Baixar Arquivo HTML",
                data=html_content,
                file_name=nome_html,
                mime="text/html"
            )

        st.session_state.html_content = html_content

    else:

        row2_1 = st.columns(4)

        with row2_1[0]:

            enviar_informes = st.button(f'Enviar Informes Gerais')

            if enviar_informes:

                puxar_aba_simples(st.session_state.id_gsheet, 'Telefones Guias', 'df_telefones')

                lista_htmls = []

                lista_telefones = []

                for guia_ref in lista_guias:

                    telefone_guia = verificar_guia_sem_telefone(st.session_state.id_gsheet, guia_ref, st.session_state.df_telefones['Guias'].unique().tolist())

                    df_pag_guia = st.session_state.df_pag_final[st.session_state.df_pag_final['Guia']==guia_ref].sort_values(by=['Data da Escala', 'Veículo', 'Motorista']).reset_index(drop=True)

                    df_pag_guia['Data da Escala'] = pd.to_datetime(df_pag_guia['Data da Escala'])

                    df_pag_guia['Data da Escala'] = df_pag_guia['Data da Escala'].dt.strftime('%d/%m/%Y')

                    soma_servicos = df_pag_guia['Valor Total'].sum()

                    soma_servicos = format_currency(soma_servicos, 'BRL', locale='pt_BR')

                    for item in ['Valor', 'Acréscimo Motoguia', 'Desconto por Junção', 'Valor Total']:

                        df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

                    html = definir_html(df_pag_guia)

                    nome_html = f'{guia_ref}.html'

                    criar_output_html(nome_html, html, guia_ref, soma_servicos)

                    with open(nome_html, "r", encoding="utf-8") as file:

                        html_content_guia_ref = file.read()

                    lista_htmls.append([html_content_guia_ref, telefone_guia])

                webhook_thiago = "https://conexao.multiatend.com.br/webhook/pagamentoluckjoaopessoa"

                payload = {"informe_html": lista_htmls}
                
                response = requests.post(webhook_thiago, json=payload)
                    
                if response.status_code == 200:
                    
                    st.success(f"Mapas de Pagamentos enviados com sucesso!")
                    
                else:
                    
                    st.error(f"Erro. Favor contactar o suporte")

                    st.error(f"{response}")

            else:

                nome_html = f'Mapas Guias Geral.html'

                criar_output_html_geral(nome_html)

                for guia_ref in lista_guias:

                    df_pag_guia = st.session_state.df_pag_final[st.session_state.df_pag_final['Guia']==guia_ref].sort_values(by=['Data da Escala', 'Veículo', 'Motorista']).reset_index(drop=True)

                    soma_servicos = df_pag_guia['Valor Total'].sum()

                    soma_servicos = format_currency(soma_servicos, 'BRL', locale='pt_BR')

                    html = definir_html(df_pag_guia)

                    inserir_html(nome_html, html, guia_ref, soma_servicos)

                with open(nome_html, "r", encoding="utf-8") as file:

                    html_content = file.read()

                with row2_1[1]:

                    st.download_button(
                        label="Baixar Arquivo HTML - Geral",
                        data=html_content,
                        file_name=nome_html,
                        mime="text/html"
                    )

if 'html_content' in st.session_state and guia:

    with row2_1[2]:

        enviar_informes = st.button(f'Enviar Informes | {guia}')

    if enviar_informes:

        puxar_aba_simples(st.session_state.id_gsheet, 'Telefones Guias', 'df_telefones')

        telefone_guia = verificar_guia_sem_telefone(st.session_state.id_gsheet, guia, st.session_state.df_telefones['Guias'].unique().tolist())

        webhook_thiago = "https://conexao.multiatend.com.br/webhook/pagamentoluckjoaopessoa"
        
        payload = {"informe_html": st.session_state.html_content, 
                    "telefone": telefone_guia}
        
        response = requests.post(webhook_thiago, json=payload)
            
        if response.status_code == 200:
            
            st.success(f"Mapas de Pagamento enviados com sucesso!")
            
        else:
            
            st.error(f"Erro. Favor contactar o suporte")

            st.error(f"{response}")
