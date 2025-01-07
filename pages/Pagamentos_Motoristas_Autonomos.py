import streamlit as st
import pandas as pd
import mysql.connector
import decimal
import numpy as np
from datetime import datetime, timedelta, time
from babel.numbers import format_currency
import gspread 
import requests
from google.cloud import secretmanager 
import json
from google.oauth2.service_account import Credentials

def gerar_df_phoenix(vw_name):
    # Parametros de Login AWS
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

    st.session_state.df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Status do Servico']!='CANCELADO') & 
                                                              (~pd.isna(st.session_state.df_escalas['Escala']))].reset_index(drop=True)
    
    st.session_state.df_escalas['Data | Horario Apresentacao'] = pd.to_datetime(st.session_state.df_escalas['Data | Horario Apresentacao'], errors='coerce')
    
    st.session_state.df_escalas['Guia'] = st.session_state.df_escalas['Guia'].fillna('')

def puxar_infos_gdrive(id_gsheet, nome_df_1, aba_1, nome_df_2, aba_2, nome_df_3, aba_3):

    project_id = "grupoluck"
    secret_id = "cred-luck-aracaju"
    secret_client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_name})
    secret_payload = response.payload.data.decode("UTF-8")
    credentials_info = json.loads(secret_payload)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(id_gsheet)
    
    sheet = spreadsheet.worksheet(aba_1)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df_1] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

    st.session_state[nome_df_1]['Valor'] = pd.to_numeric(st.session_state[nome_df_1]['Valor'], errors='coerce')

    sheet = spreadsheet.worksheet(aba_2)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df_2] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

    sheet = spreadsheet.worksheet(aba_3)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df_3] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

def verificar_servicos_regiao(df_servicos, df_regiao):

    lista_servicos_sem_regiao = []

    lista_servicos = df_servicos['Servico'].unique().tolist()

    lista_servicos_com_regiao = df_regiao['Servico'].unique().tolist()

    lista_servicos_sem_regiao = [servico for servico in lista_servicos if servico not in lista_servicos_com_regiao]

    if len(lista_servicos_sem_regiao)>0:

        df_add_excel = pd.DataFrame(lista_servicos_sem_regiao)

        df_add_excel['1'] = ''

        project_id = "grupoluck"
        secret_id = "cred-luck-aracaju"
        secret_client = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
        secret_payload = response.payload.data.decode("UTF-8")
        credentials_info = json.loads(secret_payload)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        client = gspread.authorize(credentials)
        
        spreadsheet = client.open_by_key('1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E')

        sheet = spreadsheet.worksheet('BD - Passeios | Interestaduais')

        all_values = sheet.get_all_values()

        last_row = len(all_values)

        if all_values and not any(all_values[-1]):

            last_row -= 1

        data = df_add_excel.values.tolist()

        sheet.update(f"A{last_row + 1}", data)

        st.write(lista_servicos_sem_regiao)
            
        st.error('Serviços acima inseridos na aba BD - Passeios | Interestaduais. Por favor, informe a região de cada serviço na planilha e tente novamente')

        st.stop() 

def preencher_data_hora_voo_tt(df_filtrado):

    df_filtrado.loc[df_filtrado['Tipo de Servico'].isin(['TOUR', 'TRANSFER']), 'Data Voo'] = \
        df_filtrado.loc[df_filtrado['Tipo de Servico'].isin(['TOUR', 'TRANSFER']), 'Data | Horario Apresentacao'].dt.date

    df_filtrado.loc[df_filtrado['Tipo de Servico'].isin(['TOUR', 'TRANSFER']), 'Horario Voo'] = \
        df_filtrado.loc[df_filtrado['Tipo de Servico'].isin(['TOUR', 'TRANSFER']), 'Data | Horario Apresentacao'].dt.time

    df_filtrado = df_filtrado.rename(columns={'Veiculo': 'Veículo'})

    df_filtrado['Horario Voo'] = pd.to_datetime(df_filtrado['Horario Voo'], format='%H:%M:%S').dt.time

    df_filtrado['Data | Horario Voo'] = pd.to_datetime(df_filtrado['Data Voo'].astype(str) + ' ' + df_filtrado['Horario Voo'].astype(str))

    return df_filtrado

def verificar_veiculos_sem_diaria(lista_veiculos_sem_diaria, df_filtrado):

    lista_veiculos_sem_diaria.extend(df_filtrado[pd.isna(df_filtrado['Valor'])]['Veículo'].unique().tolist())

    if len(lista_veiculos_sem_diaria)>0:

        nome_veiculos_sem_diaria = ', '.join(lista_veiculos_sem_diaria)

        st.error(f'Os veículos {nome_veiculos_sem_diaria} não tem valor de diária cadastrada. Cadastre e tente novamente, por favor')

        st.stop()

def verificar_reservas_sem_voo(df_filtrado):

    if len(df_filtrado[df_filtrado['Data Voo']==''])>0:

        lista_reservas = ', '.join(df_filtrado[df_filtrado['Data Voo']=='']['Reserva'].unique().tolist())

        df_filtrado.loc[df_filtrado['Data Voo']=='', 'Data Voo'] = df_filtrado['Data | Horario Apresentacao'].dt.date

        df_filtrado.loc[pd.isna(df_filtrado['Horario Voo']), 'Horario Voo'] = df_filtrado['Data | Horario Apresentacao'].dt.time

        st.error(f'As reservas {lista_reservas} estão sem voo no IN ou OUT. O robô vai gerar os pagamentos, criando um horário fictício para o voo')

def ajustar_data_escala_voos_madrugada(df_filtrado):

    mask_in_out_voos_madrugada = (df_filtrado['Tipo de Servico'].isin(['IN', 'OUT'])) & ((df_filtrado['Horario Voo']<=time(4,0)) | (df_filtrado['Data | Horario Apresentacao'].dt.time<=time(4,0)))

    df_filtrado.loc[mask_in_out_voos_madrugada, 'Data da Escala'] = df_filtrado.loc[mask_in_out_voos_madrugada, 'Data da Escala'] - timedelta(days=1)

    return df_filtrado

def agrupar_escalas(df_filtrado):

    df_pag_geral = df_filtrado.groupby(['Escala', 'Data da Escala', 'Modo', 'Tipo de Servico', 'Servico', 'Veículo', 'Motorista'])\
        .agg({'Data | Horario Voo': 'max', 'Data | Horario Apresentacao': 'max', 'Valor': 'max', 'Guia': 'first'}).reset_index()
    
    df_pag_geral = df_pag_geral.sort_values(by = ['Data da Escala', 'Data | Horario Apresentacao']).reset_index(drop=True)

    return df_pag_geral

def ajustar_data_tt_madrugada(df_pag_geral):

    mask_tt_madrugada = (df_pag_geral['Servico'].str.upper().str.contains('BY NIGHT|SÃO JOÃO|CATAMARÃ DO FORRÓ')) & (df_pag_geral['Tipo de Servico']=='TOUR')

    df_pag_geral.loc[mask_tt_madrugada, 'Data | Horario Voo'] = (df_pag_geral.loc[mask_tt_madrugada, 'Data | Horario Apresentacao'] + timedelta(days=1))\
        .apply(lambda dt: dt.replace(hour=1, minute=0, second=0))

    return df_pag_geral

def transformar_lista(x):

    return list(x)

def verificar_trf_apoio_ent_interestadual(df_pag_concat):

    df_pag_motoristas = df_pag_concat.groupby(['Data da Escala', 'Motorista']).agg({'Valor': 'max', 'Data | Horario Voo': 'max', 'Data | Horario Apresentacao': 'min', 'Modo': 'count'})\
        .reset_index()
    
    df_pag_motoristas = df_pag_motoristas.rename(columns = {'Modo': 'Qtd. Serviços'})

    df_pag_motoristas[['Apenas TRF/APOIO/ENTARDECER', 'Interestadual/Intermunicipal', 'Passeios sem Apoio']] = ''

    for index, value in df_pag_motoristas['Qtd. Serviços'].items():

        data_escala = df_pag_motoristas.at[index, 'Data da Escala']
        
        motorista = df_pag_motoristas.at[index, 'Motorista']
        
        df_ref = df_pag_concat[(df_pag_concat['Data da Escala']==data_escala) & (df_pag_concat['Motorista']==motorista)].reset_index(drop=True)
        
        # Deduzindo da Qtd Serviços as junções de OUT e IN, ou seja, contabilizando cada junção como apenas 1 serviço
        
        df_ref_trf = df_ref[(df_ref['Tipo de Servico']=='OUT') | (df_ref['Tipo de Servico']=='IN')].reset_index(drop=True)
        
        df_ref_trf_group = df_ref_trf.groupby(['Veículo', 'Guia', 'Motorista']).agg({'Valor': 'count', 'Tipo de Servico': transformar_lista})
        
        df_ref_trf_group = df_ref_trf_group[(df_ref_trf_group['Valor']==2) & 
                                            (df_ref_trf_group['Tipo de Servico'].apply(lambda x: all(item in x for item in ['IN', 'OUT'])))].reset_index(drop=True)
        
        if len(df_ref_trf_group)>0:
        
            out_in = int(df_ref_trf_group['Valor'].sum()/2)
        
            df_pag_motoristas.at[index, 'Qtd. Serviços'] -= out_in
        
            value = df_pag_motoristas.at[index, 'Qtd. Serviços']
        
        # Se fez mais de um serviço no dia
        
        if value > 1:
            
            lista_tipo_do_servico = df_ref['Tipo de Servico'].unique().tolist()

            lista_servico = df_ref[df_ref['Tipo de Servico']=='TOUR']['Servico'].unique().tolist()
            
            # Verifica se no dia em questão tem algum serviço do tipo TOUR
            
            if not 'TOUR' in lista_tipo_do_servico:
                
                df_pag_motoristas.at[index, 'Apenas TRF/APOIO/ENTARDECER'] = 'x'

            elif (len(lista_servico)==1 and lista_servico[0]=='ENTARDECER NA PRAIA DO JACARÉ ') or \
                (len(lista_servico)==1 and lista_servico[0]=='ALUGUEL DENTRO DE JPA') or \
                    (len(lista_servico)==2 and 'ALUGUEL DENTRO DE JPA' in lista_servico and 
                        'ENTARDECER NA PRAIA DO JACARÉ ' in lista_servico):
                
                df_pag_motoristas.at[index, 'Apenas TRF/APOIO/ENTARDECER'] = 'x'    
            
        lista_regioes = []
            
        # Verifica se teve serviço intermunicipal ou interestadual
        
        for index_2, value_2 in df_ref['Região'].items():
            
            if value_2 != 'JOÃO PESSOA':
                
                lista_regioes.append(value_2)
                
        if len(lista_regioes)>0:
            
            df_pag_motoristas.at[index, 'Interestadual/Intermunicipal'] = 'x' 

    return df_pag_motoristas

def identificar_passeios_sem_apoio(df_pag_motoristas):

    for index, value in df_pag_motoristas['Qtd. Serviços'].items():

        data_escala = df_pag_motoristas.at[index, 'Data da Escala']
        
        motorista = df_pag_motoristas.at[index, 'Motorista']
        
        df_ref = df_pag_concat[(df_pag_concat['Data da Escala']==data_escala) & (df_pag_concat['Motorista']==motorista)].reset_index(drop=True)

        for index_2, value_2 in df_ref['Servico'].items():

            if value_2 in st.session_state.df_passeios_sem_apoio['Servico'].unique().tolist():

                df_pag_motoristas.at[index, 'Passeios sem Apoio'] = 'x' 

    return df_pag_motoristas

def identificar_acrescimo_50(df_pag_motoristas):

    df_pag_motoristas['Acréscimo 50%'] = ''
    df_pag_motoristas['Valor 50%'] = 0

    # Função auxiliar para verificar e aplicar a lógica
    def verificar_acrescimo(row):
        apr_time = row['Data | Horario Apresentacao']
        voo_time = row['Data | Horario Voo']

        # Verifica se os valores não são nulos
        if pd.notna(apr_time) and pd.notna(voo_time):
            apr_time_date = apr_time.date()
            apr_time_time = apr_time.time()
            voo_time_date = voo_time.date()
            voo_time_time = voo_time.time()

            # Verifica as condições
            if (time(4) < apr_time_time <= time(18)) and (
                (voo_time_date == apr_time_date + timedelta(days=1)) or voo_time_time >= time(23, 59)
            ):
                row['Acréscimo 50%'] = 'x'
        return row

    # Aplica a função auxiliar a cada linha do DataFrame
    df_pag_motoristas = df_pag_motoristas.apply(verificar_acrescimo, axis=1)

    return df_pag_motoristas

def precificar_acrescimo_50(df_pag_motoristas, df_pag_concat):

    for index, value in df_pag_motoristas['Acréscimo 50%'].items():
        
        if value == 'x':
            
            data_escala = df_pag_motoristas.at[index, 'Data da Escala']
        
            motorista = df_pag_motoristas.at[index, 'Motorista']
            
            df_ref = df_pag_concat[(df_pag_concat['Data da Escala']==data_escala) & (df_pag_concat['Motorista']==motorista)].reset_index(drop=True)
            
            df_pag_motoristas.at[index, 'Valor 50%'] = df_ref['Valor'].iloc[-1] * 0.5

    return df_pag_motoristas

def definir_nomes_servicos_veiculos_por_dia(df_pag_motoristas, df_pag_concat):

    df_pag_motoristas['Serviços / Veículos'] = ''

    for index, value in df_pag_motoristas['Motorista'].items():
        
        str_servicos = ''
        
        data_escala = df_pag_motoristas.at[index, 'Data da Escala']
        
        df_ref = df_pag_concat[(df_pag_concat['Motorista']==value) & (df_pag_concat['Data da Escala']==data_escala)].reset_index(drop=True)
        
        for index_2, value_2 in df_ref['Servico'].items():
            
            if str_servicos == '':
                
                str_servicos = f"Serviço: {value_2} | Veículo: {df_ref.at[index_2, 'Veículo']}"
                
            else:
            
                str_servicos = f"{str_servicos}<br><br>Serviço: {value_2} | Veículo: {df_ref.at[index_2, 'Veículo']}"
                
        df_pag_motoristas.at[index, 'Serviços / Veículos'] = str_servicos

    return df_pag_motoristas

def criar_colunas_escala_veiculo_mot_guia(df_apoios):

    df_apoios[['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio']] = ''

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace('Escala Auxiliar: ', '', regex=False)

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace(' Veículo: ', '', regex=False)

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace(' Motorista: ', '', regex=False)

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace(' Guia: ', '', regex=False)

    df_apoios[['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio']] = \
        df_apoios['Apoio'].str.split(',', expand=True)
    
    return df_apoios

def transformar_em_string(apoio):

    return ', '.join(list(set(apoio.dropna())))

def criar_df_apoios():

    df_apoio_filtrado = st.session_state.df_escalas[(~pd.isna(st.session_state.df_escalas['Apoio'])) & (st.session_state.df_escalas['Data da Escala'] >= data_inicial) & 
                                                    (st.session_state.df_escalas['Data da Escala'] <= data_final)].reset_index(drop=True)
    
    df_apoio_filtrado = df_apoio_filtrado.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Motorista', 'Guia', 'Servico', 'Tipo de Servico', 'Modo'])\
        .agg({'Apoio': transformar_em_string, 'Data | Horario Apresentacao': 'min'}).reset_index()

    df_escalas_com_apoio = df_apoio_filtrado[(~df_apoio_filtrado['Apoio'].str.contains(r' \| ', regex=True))].reset_index(drop=True)

    df_escalas_com_apoio = criar_colunas_escala_veiculo_mot_guia(df_escalas_com_apoio)

    df_escalas_com_apoio = df_escalas_com_apoio[df_escalas_com_apoio['Motorista Apoio'].str.contains('MOT AUT', na=False)].reset_index(drop=True)

    df_apoios_group = df_escalas_com_apoio.groupby(['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio'])\
        .agg({'Data da Escala': 'first', 'Data | Horario Apresentacao': 'first'}).reset_index()
    
    df_apoios_group = df_apoios_group[~df_apoios_group['Motorista Apoio'].str.contains('FARIAS|GIULIANO|NETO|JUNIOR')].reset_index(drop=True)

    df_apoios_group = df_apoios_group.rename(columns={'Veiculo Apoio': 'Veículo', 'Motorista Apoio': 'Motorista', 'Guia Apoio': 'Guia', 'Escala Apoio': 'Escala'})

    df_pag_apoios = pd.merge(df_apoios_group, st.session_state.df_veiculo_categoria, on='Veículo', how='left')

    lista_veiculos_sem_diaria = df_pag_apoios[pd.isna(df_pag_apoios['Valor'])]['Veículo'].unique().tolist()

    df_pag_apoios = df_pag_apoios.sort_values(by = ['Data da Escala', 'Data | Horario Apresentacao']).reset_index(drop=True)

    df_pag_apoios['Servico']='APOIO'

    df_pag_apoios = pd.merge(df_pag_apoios, st.session_state.df_regiao, on = 'Servico', how = 'left')

    df_pag_apoios['Modo']='REGULAR'

    df_pag_apoios['Tipo de Servico']='APOIO'

    df_pag_apoios['Data | Horario Voo']=df_pag_apoios['Data | Horario Apresentacao']

    df_pag_apoios = df_pag_apoios[['Escala', 'Data da Escala', 'Modo', 'Tipo de Servico', 'Servico', 'Veículo', 'Guia', 'Motorista', 'Data | Horario Voo', 
                                'Data | Horario Apresentacao', 'Valor', 'Região']]

    df_escalas_com_2_apoios = df_apoio_filtrado[(df_apoio_filtrado['Apoio'].str.contains(r' \| ', regex=True))].reset_index(drop=True)

    df_novo = pd.DataFrame(columns=['Escala', 'Veículo', 'Motorista', 'Guia', 'Data | Horario Apresentacao', 'Data da Escala'])

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

            df_novo.at[contador, 'Veículo'] = lista_insercao[1]

            df_novo.at[contador, 'Motorista'] = lista_insercao[2]

            df_novo.at[contador, 'Guia'] = lista_insercao[3]

            df_novo.at[contador, 'Data | Horario Apresentacao'] = data_h_apr

            df_novo.at[contador, 'Data da Escala'] = data_escala

    df_novo = df_novo[df_novo['Motorista'].str.contains('MOT AUT', na=False)].reset_index(drop=True)

    df_novo = df_novo[~df_novo['Motorista'].str.contains('FARIAS|GIULIANO|NETO|JUNIOR')].reset_index(drop=True)

    df_novo = pd.merge(df_novo, st.session_state.df_veiculo_categoria, on='Veículo', how='left')

    lista_veiculos_sem_diaria.extend(df_novo[pd.isna(df_novo['Valor'])]['Veículo'].unique().tolist())

    df_novo = df_novo.sort_values(by = ['Data da Escala', 'Data | Horario Apresentacao']).reset_index(drop=True)

    df_novo['Servico']='APOIO'

    df_novo = pd.merge(df_novo, st.session_state.df_regiao, on = 'Servico', how = 'left')

    df_novo['Modo']='REGULAR'

    df_novo['Tipo de Servico']='APOIO'

    df_novo['Data | Horario Voo']=df_novo['Data | Horario Apresentacao']

    df_pag_apoios = pd.concat([df_pag_apoios, df_novo], ignore_index=True)

    return df_pag_apoios, lista_veiculos_sem_diaria

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

    project_id = "grupoluck"
    secret_id = "cred-luck-aracaju"
    secret_client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_name})
    secret_payload = response.payload.data.decode("UTF-8")
    credentials_info = json.loads(secret_payload)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
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

        df_itens_faltantes = pd.DataFrame(lista_guias, columns=['Motoristas'])

        st.dataframe(df_itens_faltantes, hide_index=True)

        project_id = "grupoluck"
        secret_id = "cred-luck-aracaju"
        secret_client = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
        secret_payload = response.payload.data.decode("UTF-8")
        credentials_info = json.loads(secret_payload)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        client = gspread.authorize(credentials)
        
        spreadsheet = client.open_by_key(id_gsheet)

        sheet = spreadsheet.worksheet('Telefones Motoristas')
        sheet_data = sheet.get_all_values()
        last_filled_row = len(sheet_data)
        data = df_itens_faltantes.values.tolist()
        start_row = last_filled_row + 1
        start_cell = f"A{start_row}"
        
        sheet.update(start_cell, data)

        st.error(f'O guia {guia} não tem número de telefone cadastrado na planilha. Ele foi inserido no final da lista de guias. Por favor, cadastre o telefone dele e tente novamente')

        st.stop()

    else:

        telefone_guia = st.session_state.df_telefones.loc[st.session_state.df_telefones['Motoristas']==guia, 'Telefone'].values[0]

    return telefone_guia

def criar_output_html_geral(nome_html):

    with open(nome_html, "w", encoding="utf-8") as file:

        pass

def inserir_html(nome_html, html, guia, soma_servicos):

    with open(nome_html, "a", encoding="utf-8") as file:

        file.write('<div style="page-break-before: always;"></div>\n')

        file.write(f'<p style="font-size:40px;">{guia}</p><br><br>')

        file.write(html)

        file.write(f'<br><br><p style="font-size:40px;">O valor total dos serviços é {soma_servicos}</p>')

def inserir_df_gsheet(df_itens_faltantes, id_gsheet, nome_aba):

    df_insercao = df_itens_faltantes.copy()

    for column in df_insercao.columns:

        if not 'Valor' in column and not 'Ajuda' in column:

            df_insercao[column] = df_insercao[column].astype(str)

    project_id = "grupoluck"
    secret_id = "cred-luck-aracaju"
    secret_client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_name})
    secret_payload = response.payload.data.decode("UTF-8")
    credentials_info = json.loads(secret_payload)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(credentials)
    
    spreadsheet = client.open_by_key(id_gsheet)

    sheet = spreadsheet.worksheet(nome_aba)

    sheet.batch_clear(["A2:Z1000"])

    data = df_insercao.values.tolist()
    sheet.update('A2', data)

st.set_page_config(layout='wide')

st.session_state.id_gsheet = '1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E'

# Puxando dados do Phoenix da 'vw_payment_guide'

if not 'df_escalas' in st.session_state:

    with st.spinner('Puxando dados do Phoenix...'):

        puxar_dados_phoenix()

# Título da página

st.title('Mapa de Pagamento - Motoristas')

st.divider()

row1 = st.columns(2)

# Objetos pra colher período do mapa

with row1[0]:

    container_datas = st.container(border=True)

    container_datas.subheader('Período')

    data_inicial = container_datas.date_input('Data Inicial', value=None ,format='DD/MM/YYYY', key='data_inicial')

    data_final = container_datas.date_input('Data Inicial', value=None ,format='DD/MM/YYYY', key='data_final')

    gerar_mapa = container_datas.button('Gerar Mapa')

# Atualizar Dados Phoenix

with row1[1]:

    row_1_1 = st.columns(3)

    with row_1_1[0]:

        atualizar_phoenix = st.button('Atualizar Dados Phoenix')

        if atualizar_phoenix:

            with st.spinner('Puxando dados do Phoenix...'):

                puxar_dados_phoenix()

st.divider()

# Script pra gerar mapa de pagamento

if data_final and data_inicial and gerar_mapa:

    # Puxando infos das planilhas

    with st.spinner('Puxando valores de diárias por veículo, ajudas de custo, passeios sem apoio...'):

        puxar_infos_gdrive(st.session_state.id_gsheet, 'df_veiculo_categoria', 'BD - Veiculo Categoria', 'df_regiao', 'BD - Passeios | Interestaduais', 'df_passeios_sem_apoio', 
                           'BD - Passeios sem Apoio')

    with st.spinner('Gerando mapas de pagamento...'):

        # Criando apoios e pegando lista de veículos no apoio que não tem valor de diária cadastrada
        
        df_pag_apoios, lista_veiculos_sem_diaria = criar_df_apoios()
    
        # Selecionando apenas os motoristas autônomos e período solicitados
    
        df_filtrado = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                                  (st.session_state.df_escalas['Motorista'].str.contains('MOT AUT', na=False))].reset_index()
        
        # Verificando se todos os serviços estão com região cadastrada pra poder gerar as ajudas de custo
        
        verificar_servicos_regiao(df_filtrado, st.session_state.df_regiao)
    
        # Preenchendo Data Voo e Horario Voo com a data e horário de apresentação
    
        df_filtrado = preencher_data_hora_voo_tt(df_filtrado)
    
        # Adicionando valor de diária por veículo
    
        df_filtrado = pd.merge(df_filtrado, st.session_state.df_veiculo_categoria, on='Veículo', how='left')
    
        # Verificando se tem veículo sem diária cadastrada
    
        verificar_veiculos_sem_diaria(lista_veiculos_sem_diaria, df_filtrado)
    
        # Verificando se existem reservas sem voo
    
        verificar_reservas_sem_voo(df_filtrado)
    
        # Diminuindo 1 dia da data da escala, quando os voos são na madrugada
    
        df_filtrado = ajustar_data_escala_voos_madrugada(df_filtrado)
    
        # Agrupando escalas
    
        df_pag_geral = agrupar_escalas(df_filtrado)
    
        # Ajustar data de passeios que terminam na madrugada
    
        df_pag_geral = ajustar_data_tt_madrugada(df_pag_geral)
    
        # Inserindo região
    
        df_pag_geral = pd.merge(df_pag_geral, st.session_state.df_regiao, on = 'Servico', how = 'left')
    
        # Juntando os apoios
    
        df_pag_concat = pd.concat([df_pag_geral, df_pag_apoios], ignore_index=True)
    
        # Verificando se fez apenas TRF/APOIO/ENTARDECER e se teve serviço Interestadual/Intermunicipal
    
        df_pag_motoristas = verificar_trf_apoio_ent_interestadual(df_pag_concat)
    
        # Identificando passeios sem ponto de apoio
    
        df_pag_motoristas = identificar_passeios_sem_apoio(df_pag_motoristas)
    
        # Identificando Acréscimo 50%
    
        df_pag_motoristas = identificar_acrescimo_50(df_pag_motoristas)
    
        # Precificando o acréscimo da diária de 50%
    
        df_pag_motoristas = precificar_acrescimo_50(df_pag_motoristas, df_pag_concat)
    
        # Precificando ajudas de custo
    
        df_pag_motoristas['Ajuda de Custo'] = df_pag_motoristas.apply(lambda row: 25 if row['Interestadual/Intermunicipal']=='x' else 
                                                                      15 if row['Apenas TRF/APOIO/ENTARDECER']=='x' or row['Passeios sem Apoio']=='x' else 0, axis=1)
        
        # Ajustando nomes de serviços e veículos utilizados por dia
    
        df_pag_motoristas = definir_nomes_servicos_veiculos_por_dia(df_pag_motoristas, df_pag_concat)
    
        # Forçando ajuda de custo de 2 reais p/ ALUGUEL FORA DE JPA
    
        df_pag_motoristas.loc[df_pag_motoristas['Serviços / Veículos'].str.contains('ALUGUEL FORA DE JPA', na=False), 'Ajuda de Custo'] = 25
    
        # Calculando Valor Total da diária
    
        df_pag_motoristas['Valor Total'] = df_pag_motoristas['Valor'] + df_pag_motoristas['Valor 50%'] + df_pag_motoristas['Ajuda de Custo']
    
        # Renomeando colunas e ajustando estética
    
        df_pag_motoristas = df_pag_motoristas.rename(columns = {'Data | Horario Voo': 'Data/Horário de Término', 
                                                                'Data | Horario Apresentacao': 'Data/Horário de Início', 'Valor': 'Valor Diária'})
    
        df_pag_motoristas = df_pag_motoristas[['Data da Escala', 'Motorista', 'Data/Horário de Início', 'Data/Horário de Término', 
                                                'Qtd. Serviços', 'Serviços / Veículos', 'Valor Diária', 'Valor 50%', 'Ajuda de Custo', 
                                                'Valor Total']]
        
        st.session_state.df_pag_motoristas = df_pag_motoristas

        st.session_state.df_pag_motoristas = st.session_state.df_pag_motoristas[(st.session_state.df_pag_motoristas['Data da Escala'] >= data_inicial) & 
            (st.session_state.df_pag_motoristas['Data da Escala'] <= data_final)].reset_index(drop=True)

        # Preenchendo aba 'BD - Mapa de Pagamento - Motoristas' no Drive

        with st.spinner('Inserindo mapas de pagamentos na planilha do Drive...'):

            inserir_df_gsheet(st.session_state.df_pag_motoristas, '1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E', 'BD - Mapa de Pagamento - Motoristas')

if 'df_pag_motoristas' in st.session_state:

    df_tabela_st = st.session_state.df_pag_motoristas.reset_index(drop=True)

    df_tabela_st['Serviços / Veículos'] = df_tabela_st['Serviços / Veículos'].astype(str).apply(lambda x: x.replace('<br><br>', ' + '))

    st.header('Gerar Mapas')

    row2 = st.columns(2)

    with row2[0]:

        lista_motoristas = st.session_state.df_pag_motoristas['Motorista'].dropna().unique().tolist()

        motorista = st.selectbox('Motorista', sorted(lista_motoristas), index=None)

    if motorista:

        row2_1 = st.columns(4)

        df_pag_motoristas_ref = st.session_state.df_pag_motoristas[st.session_state.df_pag_motoristas['Motorista']==motorista].reset_index(drop=True)

        df_tabela_st_2 = df_tabela_st[df_tabela_st['Motorista']==motorista].reset_index(drop=True)

        df_tabela_st_2['Data da Escala'] = pd.to_datetime(df_tabela_st_2['Data da Escala']).dt.strftime('%d/%m/%Y')

        df_tabela_st_2['Data/Horário de Início'] = pd.to_datetime(df_tabela_st_2['Data/Horário de Início']).dt.strftime('%d/%m/%Y %H:%M:%S')

        df_tabela_st_2['Data/Horário de Término'] = pd.to_datetime(df_tabela_st_2['Data/Horário de Término']).dt.strftime('%d/%m/%Y %H:%M:%S')

        st.dataframe(df_tabela_st_2, hide_index=True)

        with row2_1[0]:

            total_a_pagar = df_tabela_st_2['Valor Total'].sum()

            st.subheader(f'Valor Total: R${int(total_a_pagar)}')

        df_pag_motoristas_ref['Data da Escala'] = pd.to_datetime(df_pag_motoristas_ref['Data da Escala']).dt.strftime('%d/%m/%Y')

        df_pag_motoristas_ref['Data/Horário de Início'] = pd.to_datetime(df_pag_motoristas_ref['Data/Horário de Início']).dt.strftime('%d/%m/%Y %H:%M:%S')

        df_pag_motoristas_ref['Data/Horário de Término'] = pd.to_datetime(df_pag_motoristas_ref['Data/Horário de Término']).dt.strftime('%d/%m/%Y %H:%M:%S')

        soma_servicos = df_pag_motoristas_ref['Valor Total'].sum()

        soma_servicos = format_currency(soma_servicos, 'BRL', locale='pt_BR')

        for item in ['Valor Diária', 'Valor 50%', 'Ajuda de Custo', 'Valor Total']:

            df_pag_motoristas_ref[item] = df_pag_motoristas_ref[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

        html=definir_html(df_pag_motoristas_ref)

        nome_html = f'{motorista}.html'

        criar_output_html(nome_html, html, motorista, soma_servicos)

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

                puxar_aba_simples('1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E', 'Telefones Motoristas', 'df_telefones')

                lista_htmls = []

                lista_telefones = []

                for motorista_ref in lista_motoristas:

                    telefone_guia = verificar_guia_sem_telefone('1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E', motorista_ref, st.session_state.df_telefones['Motoristas'].unique().tolist())

                    df_pag_motorista = st.session_state.df_pag_motoristas[st.session_state.df_pag_motoristas['Motorista']==motorista_ref].sort_values(by=['Data da Escala']).reset_index(drop=True)

                    df_pag_motorista['Data da Escala'] = pd.to_datetime(df_pag_motorista['Data da Escala']).dt.strftime('%d/%m/%Y')

                    df_pag_motorista['Data/Horário de Início'] = pd.to_datetime(df_pag_motorista['Data/Horário de Início']).dt.strftime('%d/%m/%Y %H:%M:%S')

                    df_pag_motorista['Data/Horário de Término'] = pd.to_datetime(df_pag_motorista['Data/Horário de Término']).dt.strftime('%d/%m/%Y %H:%M:%S')

                    soma_servicos = df_pag_motorista['Valor Total'].sum()

                    soma_servicos = format_currency(soma_servicos, 'BRL', locale='pt_BR')

                    for item in ['Valor Diária', 'Valor 50%', 'Ajuda de Custo', 'Valor Total']:

                        df_pag_motorista[item] = df_pag_motorista[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

                    html = definir_html(df_pag_motorista)

                    nome_html = f'{motorista_ref}.html'

                    criar_output_html(nome_html, html, motorista_ref, soma_servicos)

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

                nome_html = f'Mapas Motoristas Geral.html'

                criar_output_html_geral(nome_html)

                for motorista_ref in lista_motoristas:

                    df_pag_guia = st.session_state.df_pag_motoristas[st.session_state.df_pag_motoristas['Motorista']==motorista_ref].sort_values(by=['Data da Escala']).reset_index(drop=True)

                    soma_servicos = df_pag_guia['Valor Total'].sum()

                    soma_servicos = format_currency(soma_servicos, 'BRL', locale='pt_BR')

                    html = definir_html(df_pag_guia)

                    inserir_html(nome_html, html, motorista_ref, soma_servicos)

                with open(nome_html, "r", encoding="utf-8") as file:

                    html_content = file.read()

                with row2_1[1]:

                    st.download_button(
                        label="Baixar Arquivo HTML - Geral",
                        data=html_content,
                        file_name=nome_html,
                        mime="text/html"
                    )
                    
if 'html_content' in st.session_state and motorista:

    with row2_1[2]:

        enviar_informes = st.button(f'Enviar Informes | {motorista}')

    if enviar_informes:

        puxar_aba_simples('1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E', 'Telefones Motoristas', 'df_telefones')

        telefone_motorista = verificar_guia_sem_telefone('1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E', motorista, st.session_state.df_telefones['Motoristas'].unique().tolist())

        webhook_thiago = "https://conexao.multiatend.com.br/webhook/pagamentoluckjoaopessoa"
        
        payload = {"informe_html": st.session_state.html_content, 
                    "telefone": telefone_motorista}
        
        response = requests.post(webhook_thiago, json=payload)
            
        if response.status_code == 200:
            
            st.success(f"Mapas de Pagamento enviados com sucesso!")
            
        else:
            
            st.error(f"Erro. Favor contactar o suporte")

            st.error(f"{response}")
