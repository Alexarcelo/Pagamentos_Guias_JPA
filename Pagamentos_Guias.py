import streamlit as st
import pandas as pd
import mysql.connector
import decimal
import numpy as np
from datetime import datetime, timedelta
from babel.numbers import format_currency
import gspread 
import requests
from google.cloud import secretmanager 
import json
from google.oauth2.service_account import Credentials

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

def verificar_servicos_tarifados(df_servicos, df_tarifario, modo_tarifario, tp_servico_tarifario):

    # Verificando se todos os serviços privativos não BA´RA estão tarifados

    lista_passeios = df_servicos['Servico'].unique().tolist()

    lista_passeios_tarifario = df_tarifario['Servico'].unique().tolist()

    lista_passeios_sem_tarifario = [item for item in lista_passeios if not item in lista_passeios_tarifario]

    lista_passeios_sem_tarifario = [item for item in lista_passeios_sem_tarifario if 'BUGGY' not in item and '4X4' not in item]

    # Se tiver serviço não tarifado, insere na planilha e manda o usuário ir lá tarifar

    if len(lista_passeios_sem_tarifario)>0:

        lista_add_excel = []

        for item in lista_passeios_sem_tarifario:

            lista_add_excel.append([item, modo_tarifario, tp_servico_tarifario, 0])

        df_add_excel = pd.DataFrame(lista_add_excel, columns=['Servico', 'Modo', 'Tipo do Servico', 'Valor'])

        # GCP projeto onde está a chave credencial
        project_id = "grupoluck"
    
        # ID da chave credencial do google.
        secret_id = "cred-luck-aracaju"
    
        # Cria o cliente.
        secret_client = secretmanager.SecretManagerServiceClient()
    
        secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
    
        secret_payload = response.payload.data.decode("UTF-8")
    
        credentials_info = json.loads(secret_payload)
    
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    
        # Use the credentials to authorize the gspread client
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        client = gspread.authorize(credentials)
        
        spreadsheet = client.open_by_key('1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E')

        sheet = spreadsheet.worksheet('Tarifario')

        all_values = sheet.get_all_values()

        last_row = len(all_values)

        if all_values and not any(all_values[-1]):

            last_row -= 1

        data = df_add_excel.values.tolist()

        sheet.update(f"A{last_row + 1}", data)

        st.dataframe(df_add_excel, hide_index=True)

        st.error('Os serviços acima estão sem tarifário cadastrado na planilha. Eles foram inseridos no final da lista da aba ' +
                 'Tarifário. Por favor, atualize os tarifários no excel e tente novamente')

        st.stop()

def gerar_pag_motoguia(df):

    df['Acréscimo Motoguia'] = np.where(df['Guia'] == df['Motorista'], df['Valor'] * 0.5, 0)

    return df

def criar_coluna_valor_total(df):
        
        if 'Desconto por Junção' in df.columns.tolist():

            df['Valor'] = df['Valor'].fillna(0)

            df['Acréscimo Motoguia'] = df['Acréscimo Motoguia'].fillna(0)

            df['Desconto por Junção'] = df['Desconto por Junção'].fillna(0) 

            df['Valor Total'] = df['Valor'] + df['Acréscimo Motoguia'] + df['Desconto por Junção']

        else:

            df['Valor'] = df['Valor'].fillna(0)

            df['Acréscimo Motoguia'] = df['Acréscimo Motoguia'].fillna(0) 

            df['Valor Total'] = df['Valor'] + df['Acréscimo Motoguia']

        return df

def ajustar_pag_giuliano_junior_neto(df):
    for index, value in df['Guia'].items():
        # Verificar se a coluna 'Estabelecimento' existe no dataframe
        if 'Estabelecimento' in df.columns:
            if ((value == 'GIULIANO - GUIA') | (value == 'JUNIOR BUGUEIRO - GUIA') | (value == 'HERBET - GUIA')) & \
            (df.at[index, 'Acréscimo Motoguia'] != 0) & \
            (df.at[index, 'Estabelecimento'] != 'BA´RA HOTEL ( - )') & \
            ((df.at[index, 'Valor Total'] < 150) | (pd.isna(df.at[index, 'Valor Total']))):

                df.at[index, 'Valor'] = 150
                df.at[index, 'Acréscimo Motoguia'] = 0
                df.at[index, 'Valor Total'] = 150

            elif (value == 'NETO VIANA - GUIA') & \
            (df.at[index, 'Acréscimo Motoguia'] != 0) & \
            (df.at[index, 'Estabelecimento'] != 'BA´RA HOTEL ( - )') & \
            ((df.at[index, 'Valor Total'] < 200) | (pd.isna(df.at[index, 'Valor Total']))):

                df.at[index, 'Valor'] = 200
                df.at[index, 'Acréscimo Motoguia'] = 0
                df.at[index, 'Valor Total'] = 200
        else:
            if ((value == 'GIULIANO - GUIA') | (value == 'JUNIOR BUGUEIRO - GUIA') | (value == 'HERBET - GUIA')) & \
            (df.at[index, 'Acréscimo Motoguia'] != 0) & \
            ((df.at[index, 'Valor Total'] < 150) | (pd.isna(df.at[index, 'Valor Total']))):

                df.at[index, 'Valor'] = 150
                df.at[index, 'Acréscimo Motoguia'] = 0
                df.at[index, 'Valor Total'] = 150

            elif (value == 'NETO VIANA - GUIA') & \
            (df.at[index, 'Acréscimo Motoguia'] != 0) & \
            ((df.at[index, 'Valor Total'] < 150) | (pd.isna(df.at[index, 'Valor Total']))):

                df.at[index, 'Valor'] = 200
                df.at[index, 'Acréscimo Motoguia'] = 0
                df.at[index, 'Valor Total'] = 200

    return df

def ajustar_horario_apr_in(data_hora):

    if data_hora.time() >= pd.to_datetime('00:00:00').time() and data_hora.time() <= pd.to_datetime('08:00:00').time():

        return data_hora + pd.Timedelta(days=1)
    else:

        return data_hora

def diurno_ou_noturno(row):

    if row['Tipo de Servico']=='IN':

        hora = row['Data | Horario Apresentacao'].time()

        if (hora >= pd.to_datetime('23:00:00').time()) | (pd.to_datetime('00:00:00').time() <= hora <= pd.to_datetime('07:00:00').time()):

            return 'MADRUGADA'
        
        else:

            return 'DIURNO'
        
    elif row['Tipo de Servico']=='OUT':

        hora = row['Data | Horario Voo'].time()

        if (hora >= pd.to_datetime('23:00:00').time()) | (pd.to_datetime('00:00:00').time() <= hora <= pd.to_datetime('07:00:00').time()):

            return 'MADRUGADA'
        
        else:

            return 'DIURNO'

def gerar_dataframe_pagamento(df_servicos, df_tarifario):

    df_pag = pd.merge(df_servicos, df_tarifario[['Servico', 'Valor']], on = 'Servico', how = 'left')

    df_pag = gerar_pag_motoguia(df_pag)

    return df_pag

def verificar_juncoes_in_out(df_servicos):

    # Acumular dados em um DataFrame

    df_pag_final = pd.DataFrame()

    # Adicionar a coluna 'Desconto por Junção' inicializada com 0

    df_servicos['Desconto por Junção'] = 0

    # Itera cada guia do df pra poder pegar as junções corretamente

    for guia in df_servicos['Guia'].unique().tolist():
        
        df = df_servicos[df_servicos['Guia']==guia].reset_index(drop=True)

        # Iterar sobre as linhas do DataFrame a partir do índice 1
        
        for index in range(1, len(df)):
            
            # Se for 'IN', o serviço anterior for 'OUT' e o Guia, Motorista e Veículo dos dois 
            # serviços forem iguais
            
            if (df.at[index, 'Tipo de Servico'] == 'IN' and
                df.at[index - 1, 'Tipo de Servico'] == 'OUT' and
                df.at[index, 'Guia'] == df.at[index - 1, 'Guia'] and
                df.at[index, 'Motorista'] == df.at[index - 1, 'Motorista'] and
                df.at[index, 'Veiculo'] == df.at[index - 1, 'Veiculo']):
                
                # Aplicar o desconto na linha atual
                
                df.at[index, 'Desconto por Junção'] = -df.at[index, 'Valor']\
                -df.at[index, 'Acréscimo Motoguia']
                

        # Adicionar os dados ao DataFrame acumulado
        
        df_pag_final = pd.concat([df_pag_final, df], ignore_index=True)

    return df_pag_final

def puxar_tarifarios():

    # GCP projeto onde está a chave credencial
    project_id = "grupoluck"

    # ID da chave credencial do google.
    secret_id = "cred-luck-aracaju"

    # Cria o cliente.
    secret_client = secretmanager.SecretManagerServiceClient()

    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_name})

    secret_payload = response.payload.data.decode("UTF-8")

    credentials_info = json.loads(secret_payload)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    # Use the credentials to authorize the gspread client
    credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key('1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E')
    
    sheet = spreadsheet.worksheet('Tarifario')

    sheet_data = sheet.get_all_values()

    st.session_state.df_tarifario = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

    st.session_state.df_tarifario_pvt_tour = \
        st.session_state.df_tarifario[(st.session_state.df_tarifario['Modo']=='PRIVATIVO') & 
                                      (st.session_state.df_tarifario['Tipo do Servico']=='TOUR/TRANSFER')].reset_index(drop=True)

    st.session_state.df_tarifario_pvt_tour['Valor'] = pd.to_numeric(st.session_state.df_tarifario_pvt_tour['Valor'], errors='coerce')

    st.session_state.df_tarifario_pvt_bara_tour = \
        st.session_state.df_tarifario[(st.session_state.df_tarifario['Modo']=='PRIVATIVO BARA') & 
                                      (st.session_state.df_tarifario['Tipo do Servico']=='TOUR/TRANSFER')].reset_index(drop=True)

    st.session_state.df_tarifario_pvt_bara_tour['Valor'] = \
    pd.to_numeric(st.session_state.df_tarifario_pvt_bara_tour['Valor'], errors='coerce')

    st.session_state.df_tarifario_reg_tour = \
        st.session_state.df_tarifario[(st.session_state.df_tarifario['Modo']=='REGULAR') & 
                                      (st.session_state.df_tarifario['Tipo do Servico']=='TOUR/TRANSFER')].reset_index(drop=True)

    st.session_state.df_tarifario_reg_tour['Valor'] = \
    pd.to_numeric(st.session_state.df_tarifario_reg_tour['Valor'], errors='coerce')

    st.session_state.df_tarifario_in_out_diurno = \
        st.session_state.df_tarifario[st.session_state.df_tarifario['Modo']=='TRANSFER DIURNO'].reset_index(drop=True)

    st.session_state.df_tarifario_in_out_diurno['Valor'] = \
    pd.to_numeric(st.session_state.df_tarifario_in_out_diurno['Valor'], errors='coerce')

    st.session_state.df_tarifario_in_out_madrugada = \
        st.session_state.df_tarifario[st.session_state.df_tarifario['Modo']=='TRANSFER MADRUGADA'].reset_index(drop=True)

    st.session_state.df_tarifario_in_out_madrugada['Valor'] = \
    pd.to_numeric(st.session_state.df_tarifario_in_out_madrugada['Valor'], errors='coerce')

    st.session_state.df_tarifario_in_out_interestadual = \
        st.session_state.df_tarifario[st.session_state.df_tarifario['Modo']=='TRANSFER INTERESTADUAL'].reset_index(drop=True)

    st.session_state.df_tarifario_in_out_interestadual['Valor'] = \
    pd.to_numeric(st.session_state.df_tarifario_in_out_interestadual['Valor'], errors='coerce')

def inserir_mapa_sheets(df_pag_final):

    # GCP projeto onde está a chave credencial
    project_id = "grupoluck"

    # ID da chave credencial do google.
    secret_id = "cred-luck-aracaju"

    # Cria o cliente.
    secret_client = secretmanager.SecretManagerServiceClient()

    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_name})

    secret_payload = response.payload.data.decode("UTF-8")

    credentials_info = json.loads(secret_payload)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    # Use the credentials to authorize the gspread client
    credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key('1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E')
    
    sheet = spreadsheet.worksheet('BD - Mapa de Pagamento - Guias')

    sheet.batch_clear(["2:100000"])

    df_pag_final = df_pag_final.fillna("").astype(str)

    data_to_insert = df_pag_final.values.tolist()

    sheet.update("A2", data_to_insert)
    
    st.success('Informações de Pagamentos inseridas na planilha!')

def gerar_dataframes_base(df_filtrado):
            
    # Gerando dataframes base para os mapas de pagamento

        # PRIVATIVO | BA´RA | TOUR | TRANSFER
    
    df_guias_pvt_tour_bara = df_filtrado[(df_filtrado['Modo']!='REGULAR') & (df_filtrado['Est. Origem']=="BA´RA HOTEL") & 
                                        ((df_filtrado['Tipo de Servico']=='TOUR') | 
                                        (df_filtrado['Tipo de Servico']=='TRANSFER'))].reset_index(drop=True)
    
        # PRIVATIVO | não BA´RA | TOUR | TRANSFER

    df_guias_pvt_tour = df_filtrado[(df_filtrado['Modo']!='REGULAR') & (df_filtrado['Est. Origem']!="BA´RA HOTEL") & 
                                    ((df_filtrado['Tipo de Servico']=='TOUR') | 
                                    (df_filtrado['Tipo de Servico']=='TRANSFER'))].reset_index(drop=True)
    
        # REGULAR | TOUR | TRANSFER

    df_guias_reg_tour = df_filtrado[(df_filtrado['Modo']=='REGULAR') & 
                                    ((df_filtrado['Tipo de Servico']=='TOUR') | 
                                    (df_filtrado['Tipo de Servico']=='TRANSFER'))].reset_index(drop=True)
    
        # IN | OUT

    df_guias_in_out = df_filtrado[(df_filtrado['Tipo de Servico']=='IN') | 
                                (df_filtrado['Tipo de Servico']=='OUT')].reset_index(drop=True)
    
    return df_guias_pvt_tour_bara, df_guias_pvt_tour, df_guias_reg_tour, df_guias_in_out

def ajustar_valor_transferistas(df_pag_guias_in_out_final, transferistas):

    for index, value in df_pag_guias_in_out_final['Guia'].items():

        if (value in transferistas) & (df_pag_guias_in_out_final.at[index, 'Valor Total'] < 85) & \
        (df_pag_guias_in_out_final.at[index, 'Valor Total'] != 0):
            
            df_pag_guias_in_out_final.at[index, 'Valor'] = 85
            df_pag_guias_in_out_final.at[index, 'Acréscimo Motoguia'] = 0
            df_pag_guias_in_out_final.at[index, 'Valor Total'] = 85   
            
        elif (value in transferistas) & (df_pag_guias_in_out_final.at[index, 'Valor Total'] == 0):
            
            df_pag_guias_in_out_final.at[index, 'Valor'] = 85
            df_pag_guias_in_out_final.at[index, 'Acréscimo Motoguia'] = 0
            df_pag_guias_in_out_final.at[index, 'Desconto por Junção'] = -85

    return df_pag_guias_in_out_final

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

    df_apoios_group[['Valor']]=25

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

    # GCP projeto onde está a chave credencial
    project_id = "grupoluck"

    # ID da chave credencial do google.
    secret_id = "cred-luck-aracaju"

    # Cria o cliente.
    secret_client = secretmanager.SecretManagerServiceClient()

    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_name})

    secret_payload = response.payload.data.decode("UTF-8")

    credentials_info = json.loads(secret_payload)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    # Use the credentials to authorize the gspread client
    credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
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

        # GCP projeto onde está a chave credencial
        project_id = "grupoluck"
    
        # ID da chave credencial do google.
        secret_id = "cred-luck-aracaju"
    
        # Cria o cliente.
        secret_client = secretmanager.SecretManagerServiceClient()
    
        secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
    
        secret_payload = response.payload.data.decode("UTF-8")
    
        credentials_info = json.loads(secret_payload)
    
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    
        # Use the credentials to authorize the gspread client
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
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

def puxar_dados_phoenix():

    st.session_state.df_escalas = gerar_df_phoenix('vw_payment_guide')

    st.session_state.df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Status do Servico']!='CANCELADO') & (~pd.isna(st.session_state.df_escalas['Escala']))].reset_index(drop=True)
    
    st.session_state.df_escalas['Data | Horario Apresentacao'] = pd.to_datetime(st.session_state.df_escalas['Data | Horario Apresentacao'], errors='coerce')

    st.session_state.df_escalas.loc[(st.session_state.df_escalas['Modo']=='PRIVATIVO POR VEICULO') | (st.session_state.df_escalas['Modo']=='PRIVATIVO POR PESSOA'), 'Modo'] = 'PRIVATIVO'
    
    st.session_state.df_escalas['Guia'] = st.session_state.df_escalas['Guia'].fillna('')

def verificar_apoios_duplicados(df_apoio_filtrado):
    
    df_apoios_duplicados = df_apoio_filtrado[df_apoio_filtrado['Apoio'].str.contains(r' \| ', regex=True)]

    df_apoios_duplicados['data_escala'] = pd.to_datetime(df_apoios_duplicados['Data da Escala']).dt.strftime('%d/%m/%Y') + ' | ' + df_apoios_duplicados['Escala']

    if len(df_apoios_duplicados['data_escala'].unique().tolist())>0:

        nomes_data_escalas = ', '.join(df_apoios_duplicados['data_escala'].unique().tolist())

        st.error(f'As escalas {nomes_data_escalas} possuem escala auxiliar duplicada')

        st.stop()

st.set_page_config(layout='wide')

# Puxando dados do Phoenix da 'vw_payment_guide'

if not 'df_escalas' in st.session_state:

    with st.spinner('Puxando dados do Phoenix...'):

        puxar_dados_phoenix()

# Definindo tarifários definidos na planilha

# Título da página

st.title('Mapa de Pagamento - Guias')

st.divider()

row1 = st.columns(2)

# Objetos pra colher período do mapa

with row1[0]:

    container_datas = st.container(border=True)

    container_datas.subheader('Período')

    data_inicial = container_datas.date_input('Data Inicial', value=None ,format='DD/MM/YYYY', key='data_inicial')

    data_final = container_datas.date_input('Data Inicial', value=None ,format='DD/MM/YYYY', key='data_final')

# Botão pra atualizar dados do phoenix em 'st.session_state.df_escalas' e botão pra atualizar tarifários

with row1[1]:

    row_1_1 = st.columns(2)

    # Botão 'Atualizar Dados Phoenix'

    with row_1_1[0]:

        atualizar_phoenix = st.button('Atualizar Dados Phoenix')

        if atualizar_phoenix:

            puxar_dados_phoenix()

st.divider()

# Script pra gerar mapa de pagamento

if data_final and data_inicial:

    with row1[1]:

        # Filtrando datas definidas pelo usuário e agrupando por escala

        df_filtrado = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & 
                                                  (st.session_state.df_escalas['Data da Escala'] <= data_final)]\
                                                    .groupby('Escala').first().reset_index()

        lista_guias = [item for item in df_filtrado['Guia'].unique().tolist() if item!='']

        container_transferistas = st.container(border=True)

        transferistas = container_transferistas.multiselect('Selecione os transferistas', sorted(lista_guias), default=None)

    if transferistas:

        with row1[0]:

            gerar_mapa = container_datas.button('Gerar Mapa de Pagamentos')

        if gerar_mapa:

            puxar_tarifarios()

            # Gerando dataframes base para os mapas de pagamento

            df_guias_pvt_tour_bara, df_guias_pvt_tour, df_guias_reg_tour, df_guias_in_out = gerar_dataframes_base(df_filtrado)
            
            # Verificando se todos os TOURS e TRANSFERS privativos BA´RA estão tarifados
            
            verificar_servicos_tarifados(df_guias_pvt_tour_bara, st.session_state.df_tarifario_pvt_bara_tour, 'PRIVATIVO BARA', 'TOUR/TRANSFER')

            st.success('Todos os TOURS e TRANSFERS privativos BA´RA estão tarifados!')

            # Verificando se todos os TOURS e TRANSFERS privativos não BA´RA estão tarifados

            verificar_servicos_tarifados(df_guias_pvt_tour, st.session_state.df_tarifario_pvt_tour, 'PRIVATIVO', 'TOUR/TRANSFER')

            st.success('Todos os TOURS e TRANSFERS privativos não BA´RA estão tarifados!')

            # Verificando se todos os TOURS e TRANSFERS regulares estão tarifados

            verificar_servicos_tarifados(df_guias_reg_tour, st.session_state.df_tarifario_reg_tour, 'REGULAR', 'TOUR/TRANSFER')

            st.success('Todos os TOURS e TRANSFERS regulares estão tarifados!')

            # Colocando valor dos TOURS e TRANSFERS privativos BA´RA
            
            df_pag_guias_pvt_tour_bara = gerar_dataframe_pagamento(df_guias_pvt_tour_bara, st.session_state.df_tarifario_pvt_bara_tour)

            # Colocando valor dos TOURS e TRANSFERS privativos não BA´RA

            df_pag_guias_pvt_tour = gerar_dataframe_pagamento(df_guias_pvt_tour, st.session_state.df_tarifario_pvt_tour)

            # Colocando valor dos TOURS e TRANSFERS regulares

            df_pag_guias_reg_tour = gerar_dataframe_pagamento(df_guias_reg_tour, st.session_state.df_tarifario_reg_tour)

            # Concatenando 'df_pag_guias_reg_tour', 'df_pag_guias_pvt_tour' e 'df_pag_guias_pvt_tour_bara' em um único dataframe

            df_pag_guias_tour_total = pd.concat([df_pag_guias_reg_tour, df_pag_guias_pvt_tour, df_pag_guias_pvt_tour_bara], 
                                                ignore_index=True)

            # Criando coluna de Valor Total e ordenando por Guia e Data da Escala

            df_pag_guias_tour_total = criar_coluna_valor_total(df_pag_guias_tour_total)

            df_pag_guias_tour_total = df_pag_guias_tour_total.sort_values(by = ['Guia', 'Data da Escala']).reset_index(drop=True)

            # Deixando apenas BA´RA HOTEL na coluna Est. Origem quando o serviço não for regular

            df_pag_guias_tour_total.loc[(df_pag_guias_tour_total['Est. Origem'] != 'BA´RA HOTEL') | 
                                        (df_pag_guias_tour_total['Modo'] == 'REGULAR'), 'Est. Origem'] = ''
            
            # Ajustando pagamento de Giuliano, Junior e Neto
            
            df_pag_guias_tour_total = ajustar_pag_giuliano_junior_neto(df_pag_guias_tour_total)

            # Criando coluna 'Data | Horario Voo' no df_guias_in_out

            df_guias_in_out['Data | Horario Voo'] = pd.to_datetime(df_guias_in_out['Data Voo'] + ' ' + df_guias_in_out['Horario Voo'])

            # Colocando 'Data | Horario Apresentacao' igual a 'Data | Horario Voo' nos INs

            df_guias_in_out.loc[(df_guias_in_out['Tipo de Servico']=='IN'), 'Data | Horario Apresentacao'] = \
                df_guias_in_out['Data | Horario Voo']

            # Classificando voos Diurnos e Madrugadas

            df_guias_in_out['Diurno / Madrugada'] = ''

            df_guias_in_out['Diurno / Madrugada'] = df_guias_in_out.apply(diurno_ou_noturno, axis=1)

            # Gerando dataframe de pagamento de transfers diurnos

            df_pag_guias_in_out_jpa_diurno = df_guias_in_out[((df_guias_in_out['Servico'].str.contains('AEROPORTO JOÃO PESSOA')) | 
                                                            (df_guias_in_out['Servico'].str.contains('GUIA BASE'))) & 
                                                            (df_guias_in_out['Diurno / Madrugada']=='DIURNO')].reset_index(drop=True)
            
            df_pag_guias_in_out_jpa_diurno = gerar_dataframe_pagamento(df_pag_guias_in_out_jpa_diurno, 
                                                                       st.session_state.df_tarifario_in_out_diurno)

            # Gerando dataframe de pagamento de transfers madrugadas

            df_pag_guias_in_out_jpa_madrugada = df_guias_in_out[((df_guias_in_out['Servico'].str.contains('AEROPORTO JOÃO PESSOA')) | 
                                                                (df_guias_in_out['Servico'].str.contains('GUIA BASE'))) & 
                                                                (df_guias_in_out['Diurno / Madrugada']=='MADRUGADA')]\
                                                                    .reset_index(drop=True)
            
            df_pag_guias_in_out_jpa_madrugada = gerar_dataframe_pagamento(df_pag_guias_in_out_jpa_madrugada, st.session_state.df_tarifario_in_out_madrugada)

            # Gerando dataframe de pagamento de transfers IN Interestadual

            df_pag_guias_in_interestadual = df_guias_in_out[(~df_guias_in_out['Servico'].str.contains('AEROPORTO JOÃO PESSOA')) & 
                                                            (df_guias_in_out['Servico'].str.contains('AEROPORTO')) & 
                                                            (df_guias_in_out['Tipo de Servico']=='IN')].reset_index(drop=True)
            
            df_pag_guias_in_interestadual = gerar_dataframe_pagamento(df_pag_guias_in_interestadual, 
                                                                      st.session_state.df_tarifario_in_out_interestadual)

            # Gerando dataframe de pagamento de transfers OUT Interestadual

            df_pag_guias_out_interestadual = df_guias_in_out[(~df_guias_in_out['Servico'].str.contains('AEROPORTO JOÃO PESSOA')) & 
                                                            (df_guias_in_out['Servico'].str.contains('AEROPORTO')) & 
                                                            (df_guias_in_out['Tipo de Servico']=='OUT')].reset_index(drop=True)
            
            df_pag_guias_out_interestadual = gerar_dataframe_pagamento(df_pag_guias_out_interestadual, 
                                                                       st.session_state.df_tarifario_in_out_interestadual)

            # Concatenando 'df_pag_guias_in_out_jpa_diurno', 'df_pag_guias_in_out_jpa_madrugada', 'df_pag_guias_out_interestadual' e 'df_pag_guias_in_interestadual' 
            # em um único dataframe

            df_pag_guias_in_out = pd.concat([df_pag_guias_in_out_jpa_diurno, df_pag_guias_in_out_jpa_madrugada, 
                                             df_pag_guias_in_interestadual, df_pag_guias_out_interestadual], ignore_index=True)
            
            # Diminuindo 1 dia de 'Data | Horario Apresentacao' dos OUTs quem tem o horário de apresentação >= 21:00

            df_pag_guias_in_out['Data | Horario Apresentacao'] = pd.to_datetime(df_pag_guias_in_out['Data | Horario Apresentacao'])
            
            df_pag_guias_in_out.loc[(df_pag_guias_in_out['Tipo de Servico'] == 'OUT') & 
                                    (df_pag_guias_in_out['Data | Horario Apresentacao'].dt.time >= pd.to_datetime('21:00:00').time()), 
                                    'Data | Horario Apresentacao'] = df_pag_guias_in_out['Data | Horario Apresentacao'] - \
                                        pd.Timedelta(days=1)
            
            # Ordenando por 'Guia', 'Motorista', 'Veiculo', 'Data | Horario Apresentacao'

            df_pag_guias_in_out = df_pag_guias_in_out\
                .sort_values(by = ['Guia', 'Motorista', 'Veiculo', 'Data | Horario Apresentacao']).reset_index(drop=True)

            # Verificando junções de OUTs e INs

            df_pag_guias_in_out_final = verificar_juncoes_in_out(df_pag_guias_in_out)

            # Criando coluna de Valor Total

            df_pag_guias_in_out_final = criar_coluna_valor_total(df_pag_guias_in_out_final)

            # Reordenando por 'Guia', 'Data da Escala'

            df_pag_guias_in_out_final = df_pag_guias_in_out_final.sort_values(by = ['Guia', 'Data da Escala']).reset_index(drop=True)

            # Ajustando pagamentos de Giuliano, Junior e Neto

            df_pag_guias_in_out_final = ajustar_pag_giuliano_junior_neto(df_pag_guias_in_out_final)

            # Ajustando valor mínimo de transferistas

            df_pag_guias_in_out_final = ajustar_valor_transferistas(df_pag_guias_in_out_final, transferistas)

            st.success('Mapas de pagamentos de TOURS, TRANSFERS, INs e OUTs gerados com sucesso! Agora só faltam os apoios...')

            df_pag_guias_tour_total['Desconto por Junção'] = 0

            df_pag_tour_final = df_pag_guias_tour_total[['Data da Escala', 'Modo', 'Tipo de Servico', 'Servico', 'Est. Origem', 
                                                         'Veiculo', 'Motorista', 'Guia', 'Valor', 'Acréscimo Motoguia', 
                                                         'Desconto por Junção', 'Valor Total']]

            df_pag_in_out_final = df_pag_guias_in_out_final[['Data da Escala', 'Modo', 'Tipo de Servico', 'Servico', 'Est. Origem', 
                                                             'Veiculo', 'Motorista', 'Guia', 'Valor', 'Acréscimo Motoguia', 
                                                             'Desconto por Junção', 'Valor Total']]
            
            df_pag_in_out_final['Est. Origem'] = ''

            df_apoios = st.session_state.df_escalas[(~pd.isna(st.session_state.df_escalas['Apoio'])) & 
                                                    (st.session_state.df_escalas['Data da Escala'] >= data_inicial) & 
                                                    (st.session_state.df_escalas['Data da Escala'] <= data_final)].reset_index()

            verificar_apoios_duplicados(df_apoios)
            
            df_apoios = criar_colunas_escala_veiculo_mot_guia(df_apoios)
            
            df_apoios_group = df_apoios.groupby(['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio'])\
                ['Data da Escala'].first().reset_index()
            
            df_apoios_group = preencher_colunas_df(df_apoios_group)

            df_apoios_group = df_apoios_group.rename(columns={'Veiculo Apoio': 'Veiculo', 'Motorista Apoio': 'Motorista', 
                                                              'Guia Apoio': 'Guia'})

            df_apoios_group = gerar_pag_motoguia(df_apoios_group)

            df_apoios_group = criar_coluna_valor_total(df_apoios_group)

            df_pag_apoios = df_apoios_group[['Data da Escala', 'Modo', 'Tipo de Servico', 'Servico', 'Est. Origem', 'Veiculo', 
                                             'Motorista', 'Guia', 'Valor', 'Acréscimo Motoguia', 'Desconto por Junção', 'Valor Total']]
            
            st.success('Mapas de pagamentos de Apoios gerados com sucesso!')
            
            df_pag_final = pd.concat([df_pag_tour_final, df_pag_in_out_final, df_pag_apoios], ignore_index=True)

            df_pag_final = df_pag_final.rename(columns={'Tipo de Servico': 'Tipo', 'Servico': 'Serviço', 'Est. Origem': 'Hotel', 
                                                        'Veiculo': 'Veículo'})

            st.session_state.df_pag_final = df_pag_final

            inserir_mapa_sheets(df_pag_final)

if 'df_pag_final' in st.session_state:

    st.header('Gerar Mapas')

    row2 = st.columns(2)

    with row2[0]:

        lista_guias = st.session_state.df_pag_final[~(st.session_state.df_pag_final['Guia'].isin(['SEM GUIA', 'NENHUM GUIA', ''])) & 
                                                    (~st.session_state.df_pag_final['Guia'].str.contains('BUGUEIRO')) & (~st.session_state.df_pag_final['Guia'].str.contains('PDV')) & 
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

                puxar_aba_simples('1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E', 'Telefones Guias', 'df_telefones')

                lista_htmls = []

                lista_telefones = []

                for guia_ref in lista_guias:

                    telefone_guia = verificar_guia_sem_telefone('1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E', guia_ref, st.session_state.df_telefones['Guias'].unique().tolist())

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

if 'html_content' in st.session_state and guia:

    with row2_1[2]:

        enviar_informes = st.button(f'Enviar Informes | {guia}')

    if enviar_informes:

        puxar_aba_simples('1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E', 'Telefones Guias', 'df_telefones')

        telefone_guia = verificar_guia_sem_telefone('1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E', guia, st.session_state.df_telefones['Guias'].unique().tolist())

        webhook_thiago = "https://conexao.multiatend.com.br/webhook/pagamentoluckjoaopessoa"
        
        payload = {"informe_html": st.session_state.html_content, 
                    "telefone": telefone_guia}
        
        response = requests.post(webhook_thiago, json=payload)
            
        if response.status_code == 200:
            
            st.success(f"Mapas de Pagamento enviados com sucesso!")
            
        else:
            
            st.error(f"Erro. Favor contactar o suporte")

            st.error(f"{response}")
