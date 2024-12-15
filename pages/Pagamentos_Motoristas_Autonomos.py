import streamlit as st
import pandas as pd
import mysql.connector
import decimal
import numpy as np
from datetime import datetime, timedelta
from babel.numbers import format_currency
from google.oauth2 import service_account
import gspread 
import requests

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

def ajustar_data_horario(row):
    if ('BY NIGHT' in row['Servico']) or ('SÃO JOÃO' in row['Servico']) or \
    ('CATAMARÃ DO FORRÓ' in row['Servico']):
        row['Data | Horario Voo'] = row['Data | Horario Apresentacao'] + timedelta(days=1)
        row['Data | Horario Voo'] = row['Data | Horario Voo'].replace(hour=1, minute=0, second=0)
    return row

def verificar_acrescimo(row):
    apr_time = row['Data | Horario Apresentacao']
    voo_time = row['Data | Horario Voo']
    
    # Verifica se apr_time e voo_time não são NaT
    if pd.notna(apr_time) and pd.notna(voo_time):
        # Verifica se 'Data/Horário de Apr.' é antes das 17:00:00 e 'Data/Horário de Voo' é no dia seguinte
        if ((apr_time.time() <= pd.Timestamp('18:00:00').time()) & (apr_time.time() >= pd.Timestamp('04:00:00').time())) & \
        ((voo_time.date() == apr_time.date() + pd.Timedelta(days=1)) | (voo_time.time() >= pd.Timestamp('23:59:00').time())):
            row['Acréscimo 50%'] = 'x'
    return row

def puxar_veiculo_categoria():

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key('1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E')
    
    sheet = spreadsheet.worksheet('BD - Veiculo Categoria')

    sheet_data = sheet.get_all_values()

    st.session_state.df_veiculo_categoria = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

    st.session_state.df_veiculo_categoria['Valor'] = pd.to_numeric(st.session_state.df_veiculo_categoria['Valor'], errors='coerce')

def puxar_regiao():

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key('1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E')
    
    sheet = spreadsheet.worksheet('BD - Passeios | Interestaduais')

    sheet_data = sheet.get_all_values()

    st.session_state.df_regiao = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

def puxar_passeios_sem_apoio():

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key('1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E')
    
    sheet = spreadsheet.worksheet('BD - Passeios sem Apoio')

    sheet_data = sheet.get_all_values()

    st.session_state.df_passeios_sem_apoio = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

def verificar_servicos_regiao(df_servicos, df_regiao):

    lista_servicos_sem_regiao = []

    lista_servicos = df_servicos['Servico'].unique().tolist()

    lista_servicos_com_regiao = df_regiao['Servico'].unique().tolist()

    lista_servicos_sem_regiao = [servico for servico in lista_servicos if servico not in lista_servicos_com_regiao]

    if len(lista_servicos_sem_regiao)>0:

        df_add_excel = pd.DataFrame(lista_servicos_sem_regiao)

        df_add_excel['1'] = ''

        nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
        credentials = service_account.Credentials.from_service_account_info(nome_credencial)
        scope = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = credentials.with_scopes(scope)
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

def criar_colunas_escala_veiculo_mot_guia(df_apoios):

    df_apoios[['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio']] = ''

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace('Escala Auxiliar: ', '', regex=False)

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace(' Veículo: ', '', regex=False)

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace(' Motorista: ', '', regex=False)

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace(' Guia: ', '', regex=False)

    df_apoios[['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio']] = \
        df_apoios['Apoio'].str.split(',', expand=True)
    
    return df_apoios

def inserir_mapa_sheets(df_pag_final):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key('1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E')
    
    sheet = spreadsheet.worksheet('BD - Mapa de Pagamento - Motoristas')

    sheet.batch_clear(["2:100000"])

    df_pag_final = df_pag_final.fillna("").astype(str)

    data_to_insert = df_pag_final.values.tolist()

    sheet.update("A2", data_to_insert)
    
    st.success('Informações de Pagamentos inseridas na planilha!')

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

def puxar_dados_phoenix():

    st.session_state.df_escalas = gerar_df_phoenix('vw_payment_guide')

    st.session_state.df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Status do Servico']!='CANCELADO') & 
                                                              (~pd.isna(st.session_state.df_escalas['Escala']))].reset_index(drop=True)

    st.session_state.df_escalas['Data | Horario Apresentacao'] = pd.to_datetime(st.session_state.df_escalas['Data | Horario Apresentacao'], errors='coerce')

    st.session_state.df_escalas.loc[(st.session_state.df_escalas['Modo']=='PRIVATIVO POR VEICULO') | 
                                    (st.session_state.df_escalas['Modo']=='PRIVATIVO POR PESSOA'), 'Modo'] = 'PRIVATIVO'
    
    st.session_state.df_escalas['Guia'] = st.session_state.df_escalas['Guia'].fillna('')

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

        df_itens_faltantes = pd.DataFrame(lista_guias, columns=['Motoristas'])

        st.dataframe(df_itens_faltantes, hide_index=True)

        nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
        credentials = service_account.Credentials.from_service_account_info(nome_credencial)
        scope = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = credentials.with_scopes(scope)
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

# Puxando dados do Phoenix da 'vw_payment_guide'

if not 'df_escalas' in st.session_state:

    with st.spinner('Puxando dados do Phoenix...'):

        puxar_dados_phoenix()

st.set_page_config(layout='wide')

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

with row1[1]:

    row_1_1 = st.columns(3)

    # Botão 'Atualizar Dados Phoenix'

    with row_1_1[0]:

        atualizar_phoenix = st.button('Atualizar Dados Phoenix')

        if atualizar_phoenix:

            with st.spinner('Puxando dados do Phoenix...'):

                puxar_dados_phoenix()

st.divider()

# Script pra gerar mapa de pagamento

if data_final and data_inicial and gerar_mapa:

    puxar_veiculo_categoria()

    puxar_regiao()
    
    df_apoio_filtrado = st.session_state.df_escalas[(~pd.isna(st.session_state.df_escalas['Apoio'])) & 
                                                    (st.session_state.df_escalas['Data da Escala'] >= data_inicial) & 
                                                    (st.session_state.df_escalas['Data da Escala'] <= data_final)].reset_index(drop=True)
    
    df_filtrado = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & 
                                            (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                            (st.session_state.df_escalas['Motorista'].str.contains('MOT AUT', na=False))].reset_index()
    
    
    verificar_servicos_regiao(df_filtrado, st.session_state.df_regiao)
    
    df_apoio_filtrado = criar_colunas_escala_veiculo_mot_guia(df_apoio_filtrado)

    df_apoio_filtrado = df_apoio_filtrado[df_apoio_filtrado['Motorista Apoio'].str.contains('MOT AUT', na=False)].reset_index(drop=True)

    df_apoios_group = df_apoio_filtrado.groupby(['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio'])\
        .agg({'Data da Escala': 'first', 'Data | Horario Apresentacao': 'min'}).reset_index()

    df_apoios_group = df_apoios_group[~df_apoios_group['Motorista Apoio'].str.contains('FARIAS|GIULIANO|NETO|JUNIOR')].reset_index(drop=True)

    df_apoios_group = df_apoios_group.rename(columns={'Veiculo Apoio': 'Veículo'})

    df_pag_apoios = pd.merge(df_apoios_group, st.session_state.df_veiculo_categoria, on='Veículo', how='left')

    lista_veiculos_sem_diaria = df_pag_apoios[pd.isna(df_pag_apoios['Valor'])]['Veículo'].unique().tolist()

    mask = df_filtrado['Tipo de Servico'].isin(['TOUR', 'TRANSFER'])

    df_filtrado.loc[mask, 'Data Voo'] = df_filtrado.loc[mask, 'Data | Horario Apresentacao'].dt.date

    df_filtrado.loc[mask, 'Horario Voo'] = df_filtrado.loc[mask, 'Data | Horario Apresentacao'].dt.time

    df_filtrado = df_filtrado.rename(columns={'Veiculo': 'Veículo'})

    df_filtrado = pd.merge(df_filtrado, st.session_state.df_veiculo_categoria, on='Veículo', how='left')

    lista_veiculos_sem_diaria.extend(df_filtrado[pd.isna(df_filtrado['Valor'])]['Veículo'].unique().tolist())

    if len(lista_veiculos_sem_diaria)>0:

        nome_veiculos_sem_diaria = ', '.join(lista_veiculos_sem_diaria)

        st.error(f'Os veículos {nome_veiculos_sem_diaria} não tem valor de diária cadastrada. Cadastre e tente novamente, por favor')

        st.stop()

    df_filtrado['Horario Voo'] = pd.to_datetime(df_filtrado['Horario Voo'], format='%H:%M:%S').dt.time

    if len(df_filtrado[df_filtrado['Data Voo']==''])>0:

        lista_reservas = ', '.join(df_filtrado[df_filtrado['Data Voo']=='']['Reserva'].unique().tolist())

        df_filtrado.loc[df_filtrado['Data Voo']=='', 'Data Voo'] = df_filtrado['Data | Horario Apresentacao'].dt.date

        df_filtrado.loc[pd.isna(df_filtrado['Horario Voo']), 'Horario Voo'] = df_filtrado['Data | Horario Apresentacao'].dt.time

        st.error(f'As reservas {lista_reservas} estão sem voo no IN ou OUT. O robô vai gerar os pagamentos, criando um horário fictício para o voo')

    df_filtrado['Data | Horario Voo'] = pd.to_datetime(df_filtrado['Data Voo'].astype(str) + ' ' + df_filtrado['Horario Voo'].astype(str))

    for index in range(len(df_filtrado)):

        tipo_de_servico = df_filtrado.at[index, 'Tipo de Servico']

        if tipo_de_servico!='TOUR' and tipo_de_servico!='TRANSFER':

            hora_voo = df_filtrado.at[index, 'Horario Voo']

            hora_apresentacao = df_filtrado.at[index, 'Data | Horario Apresentacao'].time()

            if (hora_voo >= pd.to_datetime('00:00:00').time() and hora_voo <= pd.to_datetime('07:00:00').time()) or \
                (hora_apresentacao >= pd.to_datetime('00:00:00').time() and hora_apresentacao <= pd.to_datetime('04:00:00').time()):

                df_filtrado.at[index, 'Data da Escala']-=pd.Timedelta(days=1)

    df_pag_geral = df_filtrado.groupby(['Escala', 'Data da Escala', 'Modo', 'Tipo de Servico', 'Servico', 'Veículo', 'Motorista'])\
        .agg({'Data | Horario Voo': 'max', 'Data | Horario Apresentacao': 'max', 'Valor': 'max', 'Guia': 'first'}).reset_index()
    
    df_pag_geral = df_pag_geral.sort_values(by = ['Data da Escala', 'Data | Horario Apresentacao']).reset_index(drop=True)

    df_pag_apoios = df_pag_apoios.sort_values(by = ['Data da Escala', 'Data | Horario Apresentacao']).reset_index(drop=True)

    df_pag_geral = df_pag_geral.apply(ajustar_data_horario, axis=1)

    df_pag_apoios['Servico']='APOIO'

    df_pag_geral = pd.merge(df_pag_geral, st.session_state.df_regiao, on = 'Servico', how = 'left')

    df_pag_apoios = pd.merge(df_pag_apoios, st.session_state.df_regiao, on = 'Servico', how = 'left')

    df_pag_apoios['Modo']='REGULAR'

    df_pag_apoios['Tipo de Servico']='APOIO'

    df_pag_apoios['Data | Horario Voo']=df_pag_apoios['Data | Horario Apresentacao']

    df_pag_apoios = df_pag_apoios.rename(columns={'Escala Apoio': 'Escala', 'Guia Apoio': 'Guia', 'Motorista Apoio': 'Motorista'})

    df_pag_apoios = df_pag_apoios[['Escala', 'Data da Escala', 'Modo', 'Tipo de Servico', 'Servico', 'Veículo', 'Guia', 'Motorista', 'Data | Horario Voo', 
                                'Data | Horario Apresentacao', 'Valor', 'Região']]
    
    df_pag_concat = pd.concat([df_pag_geral, df_pag_apoios], ignore_index=True)

    df_pag_motoristas = df_pag_concat.groupby(['Data da Escala', 'Motorista']).agg({'Valor': 'max', 'Data | Horario Voo': 'max', 
                                                                                    'Data | Horario Apresentacao': 'min', 'Modo': 'count'}).reset_index()
    
    df_pag_motoristas = df_pag_motoristas.rename(columns = {'Modo': 'Qtd. Serviços'})

    df_pag_motoristas['Apenas TRF/APOIO/ENTARDECER'] = ''
    df_pag_motoristas['Interestadual/Intermunicipal'] = ''
    df_pag_motoristas['Passeios sem Apoio'] = ''


    for index, value in df_pag_motoristas['Qtd. Serviços'].items():

        data_escala = df_pag_motoristas.at[index, 'Data da Escala']
        
        motorista = df_pag_motoristas.at[index, 'Motorista']
        
        df_ref = df_pag_concat[(df_pag_concat['Data da Escala']==data_escala) & (df_pag_concat['Motorista']==motorista)].reset_index(drop=True)
        
        # Deduzindo da Qtd Serviços as junções de OUT e IN, ou seja, contabilizando cada junção como apenas 1 serviço
        
        df_ref_trf = df_ref[(df_ref['Tipo de Servico']=='OUT') | (df_ref['Tipo de Servico']=='IN')].reset_index(drop=True)
        
        def funcao(x):
            return list(x)
        
        df_ref_trf_group = df_ref_trf.groupby(['Veículo', 'Guia', 'Motorista']).agg({'Valor': 'count', 'Tipo de Servico': funcao})
        
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


    puxar_passeios_sem_apoio()

    for index, value in df_pag_motoristas['Qtd. Serviços'].items():

        data_escala = df_pag_motoristas.at[index, 'Data da Escala']
        
        motorista = df_pag_motoristas.at[index, 'Motorista']
        
        df_ref = df_pag_concat[(df_pag_concat['Data da Escala']==data_escala) & (df_pag_concat['Motorista']==motorista)].reset_index(drop=True)

        for index_2, value_2 in df_ref['Servico'].items():

            if value_2 in st.session_state.df_passeios_sem_apoio['Servico'].unique().tolist():

                df_pag_motoristas.at[index, 'Passeios sem Apoio'] = 'x' 


    df_pag_motoristas['Acréscimo 50%'] = ''

    df_pag_motoristas = df_pag_motoristas.apply(verificar_acrescimo, axis=1)

    df_pag_motoristas['Valor 50%'] = 0

    for index, value in df_pag_motoristas['Acréscimo 50%'].items():
        
        if value == 'x':
            
            data_escala = df_pag_motoristas.at[index, 'Data da Escala']
        
            motorista = df_pag_motoristas.at[index, 'Motorista']
            
            df_ref = df_pag_concat[(df_pag_concat['Data da Escala']==data_escala) & (df_pag_concat['Motorista']==motorista)].reset_index(drop=True)
            
            df_pag_motoristas.at[index, 'Valor 50%'] = df_ref['Valor'].iloc[-1] * 0.5

    df_pag_motoristas['Ajuda de Custo'] = 0

    for index in range(len(df_pag_motoristas)):
        
        apenas_trf_apoio = df_pag_motoristas.at[index, 'Apenas TRF/APOIO/ENTARDECER']
        
        inter = df_pag_motoristas.at[index, 'Interestadual/Intermunicipal']

        passeios_sem_apoio = df_pag_motoristas.at[index, 'Passeios sem Apoio']
        
        if inter == 'x':
            
            df_pag_motoristas.at[index, 'Ajuda de Custo'] = 25
            
        elif apenas_trf_apoio == 'x' or passeios_sem_apoio == 'x':
            
            df_pag_motoristas.at[index, 'Ajuda de Custo'] = 15

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

    df_pag_motoristas.loc[df_pag_motoristas['Serviços / Veículos'].str.contains('ALUGUEL FORA DE JPA', na=False), 'Ajuda de Custo'] = 25

    df_pag_motoristas['Valor Total'] = df_pag_motoristas['Valor'] + df_pag_motoristas['Valor 50%'] + df_pag_motoristas['Ajuda de Custo']


    df_pag_motoristas = df_pag_motoristas.rename(columns = {'Data | Horario Voo': 'Data/Horário de Término', 
                                                            'Data | Horario Apresentacao': 'Data/Horário de Início', 'Valor': 'Valor Diária'})

    df_pag_motoristas = df_pag_motoristas[['Data da Escala', 'Motorista', 'Data/Horário de Início', 'Data/Horário de Término', 
                                            'Qtd. Serviços', 'Serviços / Veículos', 'Valor Diária', 'Valor 50%', 'Ajuda de Custo', 
                                            'Valor Total']]
    
    st.session_state.df_pag_motoristas = df_pag_motoristas

    inserir_mapa_sheets(df_pag_motoristas)

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