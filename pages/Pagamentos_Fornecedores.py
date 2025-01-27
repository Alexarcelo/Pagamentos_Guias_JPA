import streamlit as st
import pandas as pd
import mysql.connector
import decimal
from babel.numbers import format_currency
import gspread
from datetime import timedelta
from google.oauth2 import service_account
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from st_aggrid import AgGrid, GridOptionsBuilder
import requests

def gerar_df_phoenix(vw_name, base_luck):

    config = {
    'user': 'user_automation_jpa',
    'password': 'luck_jpa_2024',
    'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com',
    'database': base_luck
    }
    conexao = mysql.connector.connect(**config)
    cursor = conexao.cursor()

    if vw_name=='vw_sales':
        request_name = f'SELECT `Cod_Reserva`, `Data Execucao`, `Nome_Servico`, `Valor_Servico`, `Desconto_Global`, `Data_Servico` FROM {vw_name}'
    else:
        request_name = f'SELECT * FROM {vw_name}'
        
    cursor.execute(request_name)
    resultado = cursor.fetchall()
    cabecalho = [desc[0] for desc in cursor.description]
    cursor.close()
    conexao.close()
    df = pd.DataFrame(resultado, columns=cabecalho)
    df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
    return df

def puxar_dados_phoenix():

    st.session_state.df_escalas_bruto = gerar_df_phoenix('vw_pagamento_fornecedores', st.session_state.base_luck)

    st.session_state.view_phoenix = 'vw_pagamento_fornecedores'

    st.session_state.df_escalas = st.session_state.df_escalas_bruto[~(st.session_state.df_escalas_bruto['Status da Reserva'].isin(['CANCELADO', 'PENDENCIA DE IMPORTAÇÃO', 'RASCUNHO'])) & 
                                                                    ~(pd.isna(st.session_state.df_escalas_bruto['Status da Reserva'])) & ~(pd.isna(st.session_state.df_escalas_bruto['Escala']))]\
                                                                        .reset_index(drop=True)
    
    st.session_state.df_cnpj_fornecedores = st.session_state.df_escalas_bruto[~pd.isna(st.session_state.df_escalas_bruto['Fornecedor Motorista'])]\
        [['Fornecedor Motorista', 'CNPJ/CPF Fornecedor Motorista', 'Razao Social/Nome Completo Fornecedor Motorista']].drop_duplicates().reset_index(drop=True)
    
    st.session_state.df_sales = gerar_df_phoenix('vw_sales', st.session_state.base_luck)

    st.session_state.df_sales = st.session_state.df_sales[st.session_state.df_sales['Nome_Servico']!='EXTRA'].reset_index(drop=True)

    st.session_state.df_sales['Data_Servico'] = pd.to_datetime(st.session_state.df_sales['Data_Servico'], unit='s').dt.date
    
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

def tratar_colunas_df_tarifario(df):

    for coluna in df.columns:

        if coluna!='Servico':

            df[coluna] = (df[coluna].str.replace('.', '', regex=False).str.replace(',', '.', regex=False))

            df[coluna] = pd.to_numeric(df[coluna])

def puxar_tarifario_fornecedores():

    puxar_aba_simples(st.session_state.id_gsheet, 'Tarifário Fornecedores', 'df_tarifario')

    tratar_colunas_df_tarifario(st.session_state.df_tarifario)

def puxar_tarifario_bg_4x4():

    puxar_aba_simples(st.session_state.id_gsheet, 'Tarifário Buggy e 4x4', 'df_tarifario_bg_4x4')

    tratar_colunas_df_tarifario(st.session_state.df_tarifario_bg_4x4)

def inserir_config(df_itens_faltantes, id_gsheet, nome_aba):

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)
    
    spreadsheet = client.open_by_key(id_gsheet)

    sheet = spreadsheet.worksheet(nome_aba)

    sheet.batch_clear(["A2:Z1000"])

    data = df_itens_faltantes.values.tolist()
    sheet.update('A2', data)

def tratar_tipos_veiculos(df_escalas):

    dict_tp_veic = {'Monovolume': 'Utilitario', 'Ônibus': 'Bus'}

    df_escalas['Tipo Veiculo'] = df_escalas['Tipo Veiculo'].replace(dict_tp_veic)

    return df_escalas

def transformar_em_string(apoio):

    return ', '.join(list(set(apoio.dropna())))

def criar_colunas_escala_veiculo_mot_guia(df_apoios):

    df_apoios[['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio']] = ''

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace('Escala Auxiliar: ', '', regex=False)

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace(' Veículo: ', '', regex=False)

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace(' Motorista: ', '', regex=False)

    df_apoios['Apoio'] = df_apoios['Apoio'].str.replace(' Guia: ', '', regex=False)

    df_apoios[['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio']] = \
        df_apoios['Apoio'].str.split(',', expand=True)
    
    return df_apoios

def adicionar_apoios_em_dataframe(df_escalas_group):

    df_escalas_com_apoio = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                                       (~pd.isna(st.session_state.df_escalas['Apoio']))].reset_index(drop=True)
    
    df_escalas_com_apoio = df_escalas_com_apoio.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Tipo Veiculo', 'Motorista', 'Servico', 'Tipo de Servico', 'Fornecedor Motorista'])\
        .agg({'Apoio': transformar_em_string, 'Horario Voo': 'first', 'Data | Horario Apresentacao': 'min'}).reset_index()
    
    df_escalas_com_1_apoio = df_escalas_com_apoio[(df_escalas_com_apoio['Apoio']!='') & (~df_escalas_com_apoio['Apoio'].str.contains(r' \| ', regex=True))].reset_index(drop=True)

    if len(df_escalas_com_1_apoio)>0:

        df_escalas_com_1_apoio = criar_colunas_escala_veiculo_mot_guia(df_escalas_com_1_apoio)

        df_escalas_com_1_apoio = df_escalas_com_1_apoio[~(df_escalas_com_1_apoio['Veiculo Apoio'].isin(list(filter(lambda x: x != '', st.session_state.df_config['Frota'].tolist()))))]

        df_apoios_group = df_escalas_com_1_apoio.groupby(['Escala Apoio', 'Veiculo Apoio', 'Motorista Apoio', 'Guia Apoio', 'Servico'])\
            .agg({'Data da Escala': 'first', 'Data | Horario Apresentacao': 'first'}).reset_index()

        df_apoios_group = df_apoios_group.rename(columns={'Veiculo Apoio': 'Veiculo', 'Motorista Apoio': 'Motorista', 'Guia Apoio': 'Guia', 'Escala Apoio': 'Escala'})

        df_apoios_group = df_apoios_group[['Data da Escala', 'Escala', 'Veiculo', 'Motorista', 'Guia', 'Data | Horario Apresentacao']]

        df_apoios_group = df_apoios_group[(~df_apoios_group['Veiculo'].isin(list(filter(lambda x: x != '', st.session_state.df_config['Frota'].tolist()))))].reset_index(drop=True)

        df_veiculo_tp_veiculo = st.session_state.df_escalas[(st.session_state.df_escalas['Veiculo'].isin(df_apoios_group['Veiculo'].unique())) & 
                                                            (~st.session_state.df_escalas['Fornecedor Motorista'].dropna().str.upper().str.contains('DUPLICIDADE'))]\
                                                                [['Veiculo', 'Tipo Veiculo', 'Fornecedor Motorista']].drop_duplicates()

        df_apoios_group = pd.merge(df_apoios_group[['Data da Escala', 'Escala', 'Veiculo', 'Data | Horario Apresentacao']], df_veiculo_tp_veiculo, on='Veiculo', how='left')

        df_apoios_group[['Servico', 'Tipo de Servico', 'Horario Voo']] = ['APOIO', 'TRANSFER', None]

        df_escalas_pag = pd.concat([df_escalas_group, df_apoios_group], ignore_index=True)

    else:

        df_escalas_pag = df_escalas_group.copy()

    df_escalas_com_2_apoios = df_escalas_com_apoio[(df_escalas_com_apoio['Apoio']!='') & (df_escalas_com_apoio['Apoio'].str.contains(r' \| ', regex=True))].reset_index(drop=True)

    df_novo = pd.DataFrame(columns=['Escala', 'Veiculo', 'Data | Horario Apresentacao', 'Data da Escala'])

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

            df_novo.at[contador, 'Data | Horario Apresentacao'] = data_h_apr

            df_novo.at[contador, 'Data da Escala'] = data_escala

    df_novo = df_novo[(~df_novo['Veiculo'].isin(list(filter(lambda x: x != '', st.session_state.df_config['Frota'].tolist()))))].reset_index(drop=True)

    df_veiculo_tp_veiculo = st.session_state.df_escalas[(st.session_state.df_escalas['Veiculo'].isin(df_novo['Veiculo'].unique())) & 
                                                        (~st.session_state.df_escalas['Fornecedor Motorista'].dropna().str.upper().str.contains('DUPLICIDADE'))]\
                                                            [['Veiculo', 'Tipo Veiculo', 'Fornecedor Motorista']].drop_duplicates()

    df_novo = pd.merge(df_novo[['Data da Escala', 'Escala', 'Veiculo', 'Data | Horario Apresentacao']], df_veiculo_tp_veiculo, on='Veiculo', how='left')

    df_novo[['Servico', 'Tipo de Servico', 'Horario Voo']] = ['APOIO', 'TRANSFER', None]

    df_escalas_pag = pd.concat([df_escalas_pag, df_novo], ignore_index=True)

    df_escalas_pag =  tratar_tipos_veiculos(df_escalas_pag)

    return df_escalas_pag

def verificar_tarifarios(df_escalas_group, id_gsheet, aba_gsheet, df_tarifario):

    lista_passeios = df_escalas_group['Servico'].unique().tolist()

    lista_passeios_tarifario = df_tarifario['Servico'].unique().tolist()

    lista_passeios_sem_tarifario = [item for item in lista_passeios if not item in lista_passeios_tarifario]

    if len(lista_passeios_sem_tarifario)>0:

        df_itens_faltantes = pd.DataFrame(lista_passeios_sem_tarifario, columns=['Serviços'])

        st.dataframe(df_itens_faltantes, hide_index=True)

        nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
        credentials = service_account.Credentials.from_service_account_info(nome_credencial)
        scope = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = credentials.with_scopes(scope)
        client = gspread.authorize(credentials)
        
        spreadsheet = client.open_by_key(id_gsheet)

        sheet = spreadsheet.worksheet(aba_gsheet)
        sheet_data = sheet.get_all_values()
        last_filled_row = len(sheet_data)
        data = df_itens_faltantes.values.tolist()
        start_row = last_filled_row + 1
        start_cell = f"A{start_row}"
        
        sheet.update(start_cell, data)

        st.error('Os serviços acima não estão tarifados. Eles foram inseridos no final da planilha de tarifários. Por favor, tarife os serviços e tente novamente')

        st.stop()

def map_regiao(servico):

    for key, value in st.session_state.dict_conjugados.items():

        if key in servico: 

            return value
        
    return None 

def identificar_trf_conjugados(df_escalas_pag):

    st.session_state.dict_conjugados = {'HOTÉIS JOÃO PESSOA / AEROPORTO JOÃO PESSOA': 'João Pessoa', 'AEROPORTO JOÃO PESSOA / HOTEIS JOÃO PESSOA': 'João Pessoa'}

    df_escalas_pag['Regiao'] = df_escalas_pag['Servico'].apply(map_regiao)

    df_escalas_pag['Servico Conjugado'] = ''

    df_in_out = df_escalas_pag[df_escalas_pag['Servico'].isin(st.session_state.dict_conjugados)].reset_index()

    for veiculo in df_in_out['Veiculo'].unique():

        df_veiculo = df_in_out[(df_in_out['Veiculo']==veiculo)].reset_index(drop=True)

        for data_ref in df_veiculo['Data da Escala'].unique():

            df_data = df_veiculo[df_veiculo['Data da Escala']==data_ref].reset_index(drop=True)

            if len(df_data)>1 and df_data['Tipo de Servico'].nunique()>1:

                df_ref = df_data.sort_values(by=['Regiao', 'Data | Horario Apresentacao']).reset_index(drop=True)

                for index in range(1, len(df_ref), 2):

                    primeiro_trf = df_ref.at[index-1, 'Tipo de Servico']

                    segundo_trf = df_ref.at[index, 'Tipo de Servico']

                    nome_out = df_ref.at[index-1, 'Servico']

                    nome_in = df_ref.at[index, 'Servico']
                    
                    hora_out = pd.to_datetime(df_ref.at[index-1, 'Data | Horario Apresentacao'])

                    data_hora_out = hora_out.date()

                    hora_in = pd.to_datetime(df_ref.at[index, 'Horario Voo'], format='%H:%M:%S').replace(year=data_hora_out.year, month=data_hora_out.month, day=data_hora_out.day)

                    index_1 = df_ref.at[index-1, 'index']

                    index_2 = df_ref.at[index, 'index']

                    if primeiro_trf=='OUT' and segundo_trf=='IN' and hora_in - hora_out < timedelta(hours=4, minutes=15):

                        df_escalas_pag.at[index_1, 'Servico Conjugado'] = 'X'

                        df_escalas_pag.at[index_2, 'Servico Conjugado'] = 'X'

                        df_escalas_pag.at[index_1, 'Servico'] = f'{nome_out} + {nome_in}'

                        df_escalas_pag.at[index_2, 'Servico'] = f'{nome_out} + {nome_in}'

    return df_escalas_pag

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

        file.write(f'<p style="font-size:40px;">{guia}</p>')

        file.write(f'<p style="font-size:30px;">Serviços prestados entre {st.session_state.data_inicial.strftime("%d/%m/%Y")} e {st.session_state.data_final.strftime("%d/%m/%Y")}</p>')

        file.write(f'<p style="font-size:30px;">CPF / CNPJ: {st.session_state.cnpj}</p>')

        file.write(f'<p style="font-size:30px;">Razão Social / Nome Completo: {st.session_state.razao_social}</p><br><br>')

        file.write(html)

        file.write(f'<br><br><p style="font-size:30px;">O valor total dos serviços é {soma_servicos}</p>')

        file.write(f'<p style="font-size:30px;">Data de Pagamento: {st.session_state.data_pagamento.strftime("%d/%m/%Y")}</p>')

def verificar_fornecedor_sem_email(id_gsheet, guia, lista_guias_com_telefone):

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

        sheet = spreadsheet.worksheet('Contatos Fornecedores')
        sheet_data = sheet.get_all_values()
        last_filled_row = len(sheet_data)
        data = df_itens_faltantes.values.tolist()
        start_row = last_filled_row + 1
        start_cell = f"A{start_row}"
        
        sheet.update(start_cell, data)

        st.error(f'O fornecedor {guia} não tem e-mail cadastrado na planilha. Ele foi inserido no final da lista de fornecedores. Por favor, cadastre o e-mail dele e tente novamente')

        st.stop()

    else:

        telefone_guia = st.session_state.df_telefones.loc[st.session_state.df_telefones['Fornecedores']==guia, 'Contato'].values[0]

    return telefone_guia

def enviar_email_gmail(destinatarios, assunto, arquivo_html, remetente, senha, copia):
    try:
        # Configurações do servidor SMTP
        servidor = smtplib.SMTP('smtp.gmail.com', 587)
        servidor.starttls()
        servidor.login(remetente, senha)

        # Criação do e-mail
        email = MIMEMultipart()
        email['From'] = remetente
        email['To'] = ', '.join(destinatarios)
        email['CC'] = ', '.join(copia)
        email['Subject'] = assunto

        # Corpo do e-mail (renderizando o HTML)
        with open(arquivo_html, 'r', encoding='utf-8') as f:
            conteudo_html = f.read()
        email.attach(MIMEText(conteudo_html, 'html'))

        destinatarios_completos = destinatarios + copia

        # Envio do e-mail
        servidor.send_message(email, from_addr=remetente, to_addrs=destinatarios_completos)
        servidor.quit()
        st.success("E-mail enviado com sucesso!")
    except Exception as e:
        st.error(f"Erro ao enviar e-mail: {e}")

def verificar_servicos_tarifados_sem_valor(df_escalas_pag):

    if any(pd.isna(df_escalas_pag['Valor Final'])):

        st.error('Os serviços abaixo estão na lista de tarifário, mas não estão tarifados')

        df_servicos_sem_valor = df_escalas_pag[(pd.isna(df_escalas_pag['Valor Final']))][['Fornecedor Motorista', 'Tipo Veiculo', 'Servico']].drop_duplicates()

        st.dataframe(df_servicos_sem_valor, hide_index=True)

        st.stop()

def criar_output_html_email(nome_html, html, guia, soma_servicos):

    with open(nome_html, "w", encoding="utf-8") as file:

        file.write(f'<p style="font-size:40px;">{guia.title()}, segue abaixo o seu mapa de pagamento</p>')

        file.write(f'<p style="font-size:30px;">Serviços prestados entre {st.session_state.data_inicial.strftime("%d/%m/%Y")} e {st.session_state.data_final.strftime("%d/%m/%Y")}</p>')

        file.write(f'<p style="font-size:30px;">CPF / CNPJ: {st.session_state.cnpj}</p>')

        file.write(f'<p style="font-size:30px;">Razão Social / Nome Completo: {st.session_state.razao_social}</p><br><br>')

        file.write(html)

        file.write(f'<br><br><p style="font-size:30px;">O valor total dos serviços é {soma_servicos}</p>')

        file.write(f'<p style="font-size:30px;">Data de Pagamento: {st.session_state.data_pagamento.strftime("%d/%m/%Y")}</p>')

def inserir_valor_servico_desconto(df_escalas_group_bg_4x4):

    df_sales_filtrado = st.session_state.df_sales.rename(columns={'Cod_Reserva': 'Reserva', 'Nome_Servico': 'Servico', 'Valor_Servico': 'Valor Venda', 'Desconto_Global': 'Desconto Reserva'})

    df_escalas_group_bg_4x4 = pd.merge(df_escalas_group_bg_4x4, df_sales_filtrado, on=['Reserva', 'Servico'], how='left')

    return df_escalas_group_bg_4x4

def precificar_flor_das_trilhas(df_escalas_group_bg_4x4):

    df_escalas_group_bg_4x4.loc[df_escalas_group_bg_4x4['Veiculo']=='FLOR DA TRILHA', 'Valor Venda'] = 700

    df_escalas_group_bg_4x4.loc[df_escalas_group_bg_4x4['Veiculo']=='FLOR DA TRILHA', 'Desconto Reserva'] = 0

    df_escalas_group_bg_4x4.loc[df_escalas_group_bg_4x4['Veiculo']=='FLOR DA TRILHA', 'Venda Líquida de Desconto'] = 700

    return df_escalas_group_bg_4x4

def gerar_df_pag_final_forn_bg_4x4():
    
    st.session_state.df_pag_final_forn_bg_4x4 = df_escalas_group_bg_4x4[['Data da Escala', 'Reserva', 'Servico', 'Fornecedor Motorista', 'Tipo Veiculo', 'Veiculo', 'Valor Venda', 
                                                                         'Desconto Reserva', 'Venda Líquida de Desconto', 'Valor Net', 'Valor Final']]
    
    st.session_state.df_pag_final_forn_bg_4x4['Data da Escala'] = pd.to_datetime(st.session_state.df_pag_final_forn_bg_4x4['Data da Escala'])

    st.session_state.df_pag_final_forn_bg_4x4['Data da Escala'] = st.session_state.df_pag_final_forn_bg_4x4['Data da Escala'].dt.strftime('%d/%m/%Y')

    st.session_state.df_pag_final_forn_bg_4x4 = st.session_state.df_pag_final_forn_bg_4x4.drop_duplicates().reset_index(drop=True)

st.set_page_config(layout='wide')

if not 'base_luck' in st.session_state:

    st.session_state.base_luck = 'test_phoenix_joao_pessoa'

if not 'id_gsheet' in st.session_state:

    st.session_state.id_gsheet = '1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E'

if not 'id_webhook' in st.session_state:

    st.session_state.id_webhook = ''

if not 'mostrar_config' in st.session_state:

    st.session_state.mostrar_config = False

if not 'df_config' in st.session_state:

    with st.spinner('Puxando configurações...'):

        puxar_aba_simples(st.session_state.id_gsheet, 'Configurações Fornecedores', 'df_config')

if not 'df_escalas' in st.session_state or st.session_state.view_phoenix!='vw_pagamento_fornecedores':

    with st.spinner('Puxando dados do Phoenix...'):

        puxar_dados_phoenix()

st.title('Mapa de Pagamento - Fornecedores - João Pessoa')

st.divider()

st.header('Configurações')

alterar_configuracoes = st.button('Visualizar Configurações')

if alterar_configuracoes:

    if st.session_state.mostrar_config == True:

        st.session_state.mostrar_config = False

    else:

        st.session_state.mostrar_config = True

row01 = st.columns(1)

if st.session_state.mostrar_config == True:

    with row01[0]:

        st.subheader('Excluir Veículos')

        container_frota = st.container(height=300)

        filtrar_frota = container_frota.multiselect('', sorted(st.session_state.df_escalas_bruto['Veiculo'].dropna().unique().tolist()), key='filtrar_frota', 
                                       default=sorted(list(filter(lambda x: x != '', st.session_state.df_config['Frota'].tolist()))))
        
        st.subheader('Excluir Serviços')
        
        filtrar_servicos = st.multiselect('', sorted(st.session_state.df_escalas_bruto['Servico'].dropna().unique().tolist()), key='filtrar_servicos', 
                                          default=sorted(list(filter(lambda x: x != '', st.session_state.df_config['Excluir Servicos'].tolist()))))
        
        st.subheader('Mark Up Buggy | 4x4')
        
        mark_up_buggy_4x4 = st.number_input('', value=int(st.session_state.df_config['Mark Up Buggy | 4x4'].iloc[0]))

    salvar_config = st.button('Salvar Configurações')

    if salvar_config:

        with st.spinner('Salvando Configurações...'):

            lista_escolhas = [filtrar_frota, filtrar_servicos, mark_up_buggy_4x4]

            st.session_state.df_config = pd.DataFrame({f'Coluna{i+1}': pd.Series(lista) for i, lista in enumerate(lista_escolhas)})

            st.session_state.df_config = st.session_state.df_config.fillna('')

            inserir_config(st.session_state.df_config, st.session_state.id_gsheet, 'Configurações Fornecedores')

            puxar_aba_simples(st.session_state.id_gsheet, 'Configurações Fornecedores', 'df_config')

        st.session_state.mostrar_config = False

        st.rerun()

st.divider()

row1 = st.columns(2)

with row1[0]:

    container_datas = st.container(border=True)

    container_datas.subheader('Período')

    data_inicial = container_datas.date_input('Data Inicial', value=None ,format='DD/MM/YYYY', key='data_inicial')

    data_final = container_datas.date_input('Data Final', value=None ,format='DD/MM/YYYY', key='data_final')

    gerar_mapa = container_datas.button('Gerar Mapa de Pagamentos')

with row1[1]:

    atualizar_phoenix = st.button('Atualizar Dados Phoenix')

    container_data_pgto = st.container(border=True)

    container_data_pgto.subheader('Data de Pagamento')

    data_pagamento = container_data_pgto.date_input('Data de Pagamento', value=None ,format='DD/MM/YYYY', key='data_pagamento')

    if not data_pagamento:

        st.warning('Preencha a data de pagamento para visualizar os mapas de pagamentos.')

if atualizar_phoenix:

    with st.spinner('Puxando dados do Phoenix...'):

        puxar_dados_phoenix()

if gerar_mapa:

    # Puxando tarifários e tratando colunas de números

    with st.spinner('Puxando tarifários...'):

        puxar_tarifario_fornecedores()

        puxar_tarifario_bg_4x4()

    # Filtrando período solicitado pelo usuário

    df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final) & 
                                                (~st.session_state.df_escalas['Veiculo'].isin(list(filter(lambda x: x != '', st.session_state.df_config['Frota'].tolist())))) & 
                                                (~st.session_state.df_escalas['Servico'].isin(list(filter(lambda x: x != '', st.session_state.df_config['Excluir Servicos'].tolist()))))]\
                                                .reset_index(drop=True)
    
    

    # Transformando Data | Horario Apresentacao dos INs como Data | Horario Voo

    df_escalas['Data | Horario Apresentacao'] = df_escalas.apply(lambda row: pd.to_datetime(str(row['Data da Escala']) + ' ' + str(row['Horario Voo'])) 
                                                                if row['Tipo de Servico']=='IN' and not pd.isna(row['Horario Voo']) else row['Data | Horario Apresentacao'], axis=1)

    # Tratando nomes de tipos de veículos

    df_escalas = tratar_tipos_veiculos(df_escalas)

    # Excluindo Buggys e 4x4 dos pagamentos

    df_escalas_sem_buggy_4x4 = df_escalas[~df_escalas['Tipo Veiculo'].isin(['Buggy', '4X4'])].reset_index(drop=True)

    if len(df_escalas_sem_buggy_4x4)>0:

        # Agrupando escalas

        df_escalas_group = df_escalas_sem_buggy_4x4.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Tipo Veiculo', 'Servico', 'Tipo de Servico', 'Fornecedor Motorista', 'Motorista'])\
            .agg({'Horario Voo': 'first', 'Data | Horario Apresentacao': 'max', 'Total ADT': 'sum', 'Total CHD': 'sum'}).reset_index()

        # Adicionando apoios no dataframe

        df_escalas_group = adicionar_apoios_em_dataframe(df_escalas_group)
            
        # Identificando transfers conjugados

        df_escalas_group = identificar_trf_conjugados(df_escalas_group)

        # Verificando se todos os serviços estão tarifados

        verificar_tarifarios(df_escalas_group, st.session_state.id_gsheet, 'Tarifário Fornecedores', st.session_state.df_tarifario)
            
        # Colocando valores tarifarios
            
        df_escalas_pag = pd.merge(df_escalas_group, st.session_state.df_tarifario, on='Servico', how='left')

        # Gerando coluna valor levando em conta o tipo de veículo usado e se é conjugado e se é CANOPUS

        df_escalas_pag['Valor Final'] = df_escalas_pag.apply(lambda row: row[f"{row['Tipo Veiculo']} {row['Fornecedor Motorista']}"] if row['Fornecedor Motorista']!='LUCENA CANOPUS' 
                                                            else 50*(row['Total ADT'] + row['Total CHD']), axis=1)

        # Verificando se todos os serviços da planilha estão tarifados

        verificar_servicos_tarifados_sem_valor(df_escalas_pag)

        st.session_state.df_pag_final_forn = df_escalas_pag[['Data da Escala', 'Tipo de Servico', 'Servico', 'Fornecedor Motorista', 'Tipo Veiculo', 'Veiculo', 'Servico Conjugado', 'Valor Final']]

        st.session_state.df_pag_final_forn['Valor Final'] = st.session_state.df_pag_final_forn['Valor Final'].fillna(0)

    else:

        st.session_state.df_pag_final_forn = pd.DataFrame(columns=['Data da Escala', 'Tipo de Servico', 'Servico', 'Fornecedor Motorista', 'Tipo Veiculo', 'Veiculo', 'Servico Conjugado', 'Valor Final'])

    # Gerando df_escalas só de Buggys e 4x4

    df_escalas_bg_4x4 = df_escalas[df_escalas['Tipo Veiculo'].isin(['Buggy', '4X4'])].reset_index(drop=True)

    # Agrupando escalas

    df_escalas_group_bg_4x4 = df_escalas_bg_4x4.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Tipo Veiculo', 'Servico', 'Tipo de Servico', 'Fornecedor Motorista', 'Motorista'])\
        .agg({'Total ADT': 'sum', 'Total CHD': 'sum', 'Reserva': transformar_em_string}).reset_index()
    
    # Inserindo valor do serviço e desconto

    df_escalas_group_bg_4x4 = inserir_valor_servico_desconto(df_escalas_group_bg_4x4)

    # Verificando se todos os serviços estão tarifados

    verificar_tarifarios(df_escalas_group_bg_4x4, st.session_state.id_gsheet, 'Tarifário Buggy e 4x4', st.session_state.df_tarifario_bg_4x4)

    # Inserindo valores net

    df_escalas_group_bg_4x4 = pd.merge(df_escalas_group_bg_4x4, st.session_state.df_tarifario_bg_4x4, on='Servico', how='left')

    # Eliminando valores de desconto maiores que o valor da venda

    df_escalas_group_bg_4x4.loc[df_escalas_group_bg_4x4['Desconto Reserva']>df_escalas_group_bg_4x4['Valor Venda'], 'Desconto Reserva'] = 0

    # Calculando venda líquida de desconto * 70%

    df_escalas_group_bg_4x4['Venda Líquida de Desconto'] = (df_escalas_group_bg_4x4['Valor Venda']-df_escalas_group_bg_4x4['Desconto Reserva'])*0.7

    # Ajustando valor de flor das trilhas p/ 700

    df_escalas_group_bg_4x4 = precificar_flor_das_trilhas(df_escalas_group_bg_4x4)

    # Escolhendo entre valor net e venda líquida de desconto p/ gerar valor de pagamento

    df_escalas_group_bg_4x4['Valor Final'] = df_escalas_group_bg_4x4.apply(lambda row: row['Valor Net'] if row['Venda Líquida de Desconto']>row['Valor Net'] else row['Venda Líquida de Desconto'], 
                                                                           axis=1)
    
    gerar_df_pag_final_forn_bg_4x4()
    
if 'df_pag_final_forn' in st.session_state:

    st.header('Gerar Mapas')

    bg_4x4 = st.multiselect('Visulizar Apenas Buggys e 4x4', ['Sim'], default=None)

    if len(bg_4x4)==0:

        row2 = st.columns(2)

        with row2[0]:

            lista_fornecedores = ['SELECIONAR TODOS']

            lista_fornecedores.extend(sorted(st.session_state.df_pag_final_forn['Fornecedor Motorista'].dropna().unique().tolist()))

            fornecedor = st.multiselect('Fornecedores', lista_fornecedores, default=None)

        if fornecedor and data_pagamento and data_inicial and data_final:

            row2_1 = st.columns(4)

            if 'SELECIONAR TODOS' in fornecedor:

                df_pag_guia = st.session_state.df_pag_final_forn.sort_values(by=['Fornecedor Motorista', 'Data da Escala', 'Veiculo']).reset_index(drop=True)

            else:

                df_pag_guia = st.session_state.df_pag_final_forn[st.session_state.df_pag_final_forn['Fornecedor Motorista'].isin(fornecedor)].sort_values(by=['Data da Escala', 'Veiculo'])\
                    .reset_index(drop=True)
                
                st.session_state.cnpj = st.session_state.df_cnpj_fornecedores[st.session_state.df_cnpj_fornecedores['Fornecedor Motorista'].isin(fornecedor)]['CNPJ/CPF Fornecedor Motorista'].iloc[0]

                st.session_state.razao_social = st.session_state.df_cnpj_fornecedores[st.session_state.df_cnpj_fornecedores['Fornecedor Motorista'].isin(fornecedor)]\
                    ['Razao Social/Nome Completo Fornecedor Motorista'].iloc[0]

            df_pag_guia['Data da Escala'] = pd.to_datetime(df_pag_guia['Data da Escala'])

            df_pag_guia['Data da Escala'] = df_pag_guia['Data da Escala'].dt.strftime('%d/%m/%Y')

            container_dataframe = st.container()

            container_dataframe.dataframe(df_pag_guia, hide_index=True, use_container_width = True)

            with row2_1[0]:

                total_a_pagar = df_pag_guia['Valor Final'].sum()

                st.subheader(f'Valor Total: R${int(total_a_pagar)}')

            soma_servicos = df_pag_guia['Valor Final'].sum()

            soma_servicos = format_currency(soma_servicos, 'BRL', locale='pt_BR')

            for item in ['Valor Final']:

                df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

            html = definir_html(df_pag_guia)

            nome_html = f"{', '.join(fornecedor)}.html"

            nome_titulo_html = f"{', '.join(fornecedor)}"

            criar_output_html(nome_html, html, nome_titulo_html, soma_servicos)

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

        if 'html_content' in st.session_state and len(fornecedor)==1 and fornecedor[0]!='SELECIONAR TODOS' and 'df_pag_final_forn' in st.session_state and data_pagamento:

            with row2_1[2]:

                enviar_informes = st.button(f"Enviar Informes | {', '.join(fornecedor)}")

            if enviar_informes:

                puxar_aba_simples(st.session_state.id_gsheet, 'Contatos Fornecedores', 'df_telefones')

                criar_output_html_email(nome_html, html, nome_titulo_html, soma_servicos)

                email_fornecedor = verificar_fornecedor_sem_email(st.session_state.id_gsheet, fornecedor[0], st.session_state.df_telefones['Fornecedores'].unique().tolist())

                destinatarios = [email_fornecedor]

                copiados = ['financeirojpa@luckreceptivo.com.br', 'contasapagarjpa2@gmail.com', 'financeirojpa2@luckreceptivo.com.br', 'contasapagarjpa@luckreceptivo.com.br', 'contato@tourazul.com.br']
                assunto = f'Mapa de Pagamento {st.session_state.data_inicial.strftime("%d/%m/%Y")} e {st.session_state.data_final.strftime("%d/%m/%Y")}'
                remetente = 'supervlogisticajpa@luckreceptivo.com.br'
                senha = 'rcfhmfrzjbnfglmg'

                enviar_email_gmail(destinatarios, assunto, nome_html, remetente, senha, copiados)

    else:

        row2 = st.columns(5)

        with row2[2]:

            gerar_mapas_2 = st.button('Gerar Mapas Pós Descontos')

        if gerar_mapas_2:

            st.session_state.omitir_pag_final_bg_4x4 = True

        with row2[3]:

            voltar_para_alterar_descontos = st.button('Voltar p/ Alterar Descontos')

        if voltar_para_alterar_descontos:

            st.session_state.omitir_pag_final_bg_4x4 = False

        if not 'omitir_pag_final_bg_4x4' in st.session_state or st.session_state.omitir_pag_final_bg_4x4==False:

            with row2[0]:

                desconto = st.number_input('Desconto', value=None)

                alterar_desconto = st.button('Alterar Desconto')

                if alterar_desconto and st.session_state.index_escolhido:

                    st.session_state.df_pag_final_forn_bg_4x4.loc[st.session_state.index_escolhido, 'Desconto Reserva'] = desconto

                    st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Venda Líquida de Desconto'] = \
                        (st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Valor Venda']-\
                        st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Desconto Reserva'])*0.7
                    
                    if st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Venda Líquida de Desconto']>\
                        st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Valor Net']:

                        st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Valor Final'] = \
                            st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Valor Net']
                        
                    else:

                        st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Valor Final'] = \
                            st.session_state.df_pag_final_forn_bg_4x4.at[st.session_state.index_escolhido, 'Venda Líquida de Desconto']
                                                
            row_height = 32
            header_height = 56  
            num_rows = len(st.session_state.df_pag_final_forn_bg_4x4)
            height_2 = header_height + (row_height * num_rows)

            gb_2 = GridOptionsBuilder.from_dataframe(st.session_state.df_pag_final_forn_bg_4x4)
            gb_2.configure_selection('single')
            gb_2.configure_grid_options(domLayout='autoHeight')
            gridOptions_2 = gb_2.build()

            grid_response_2 = AgGrid(st.session_state.df_pag_final_forn_bg_4x4, gridOptions=gridOptions_2, enable_enterprise_modules=False, fit_columns_on_grid_load=True, height=height_2)

            if not grid_response_2['selected_rows'] is None:

                st.session_state.index_escolhido = grid_response_2['selected_rows'].reset_index()['index'].astype(int).iloc[0]

            else:

                st.session_state.index_escolhido = None

        else:

            with row2[0]:

                lista_fornecedores = ['SELECIONAR TODOS']

                lista_fornecedores.extend(sorted(st.session_state.df_pag_final_forn_bg_4x4['Fornecedor Motorista'].dropna().unique().tolist()))

                fornecedor = st.multiselect('Fornecedores', lista_fornecedores, default=None)

            if fornecedor and data_pagamento and data_inicial and data_final:

                row2_1 = st.columns(4)

                if 'SELECIONAR TODOS' in fornecedor:

                    df_pag_guia = st.session_state.df_pag_final_forn_bg_4x4.sort_values(by=['Fornecedor Motorista', 'Data da Escala', 'Veiculo']).reset_index(drop=True)

                else:

                    st.session_state.cnpj = st.session_state.df_cnpj_fornecedores[st.session_state.df_cnpj_fornecedores['Fornecedor Motorista'].isin(fornecedor)]['CNPJ/CPF Fornecedor Motorista'].iloc[0]

                    st.session_state.razao_social = st.session_state.df_cnpj_fornecedores[st.session_state.df_cnpj_fornecedores['Fornecedor Motorista'].isin(fornecedor)]\
                        ['Razao Social/Nome Completo Fornecedor Motorista'].iloc[0]

                    df_pag_guia = st.session_state.df_pag_final_forn_bg_4x4[st.session_state.df_pag_final_forn_bg_4x4['Fornecedor Motorista'].isin(fornecedor)]\
                        .sort_values(by=['Data da Escala', 'Veiculo']).reset_index(drop=True)

                container_dataframe = st.container()

                container_dataframe.dataframe(df_pag_guia, hide_index=True, use_container_width = True)

                with row2_1[0]:

                    total_a_pagar = df_pag_guia['Valor Final'].sum()

                    st.subheader(f'Valor Total: R${int(total_a_pagar)}')

                soma_servicos = df_pag_guia['Valor Final'].sum()

                soma_servicos = format_currency(soma_servicos, 'BRL', locale='pt_BR')

                for item in ['Valor Venda', 'Desconto Reserva', 'Venda Líquida de Desconto', 'Valor Net', 'Valor Final']:

                    df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR') if pd.notna(x) else x)

                html = definir_html(df_pag_guia[['Data da Escala', 'Reserva', 'Servico', 'Veiculo', 'Valor Net', 'Valor Final']])

                nome_html = f"{', '.join(fornecedor)}.html"

                nome_titulo_html = f"{', '.join(fornecedor)}"

                criar_output_html(nome_html, html, nome_titulo_html, soma_servicos)

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

            if 'html_content' in st.session_state and len(fornecedor)==1 and fornecedor[0]!='SELECIONAR TODOS' and 'df_pag_final_forn' in st.session_state and data_pagamento:

                with row2_1[2]:

                    enviar_informes = st.button(f'Enviar Informes | {fornecedor[0]}')

                if enviar_informes:

                    puxar_aba_simples(st.session_state.id_gsheet, 'Contatos Fornecedores', 'df_telefones')

                    telefone_fornecedor = verificar_fornecedor_sem_email(st.session_state.id_gsheet, fornecedor[0], st.session_state.df_telefones['Fornecedores'].unique().tolist())

                    webhook_thiago = st.session_state.id_webhook
                    
                    payload = {"informe_html": st.session_state.html_content, 
                                "telefone": telefone_fornecedor}
                    
                    response = requests.post(webhook_thiago, json=payload)
                        
                    if response.status_code == 200:
                        
                        st.success(f"Mapas de Pagamento enviados com sucesso!")
                        
                    else:
                        
                        st.error(f"Erro. Favor contactar o suporte")

                        st.error(f"{response}")
