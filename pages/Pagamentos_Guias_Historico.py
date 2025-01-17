import streamlit as st
import pandas as pd
from babel.numbers import format_currency
import gspread 
import requests
from google.cloud import secretmanager 
import json
from google.oauth2.service_account import Credentials
from google.oauth2 import service_account

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

        df_itens_faltantes = pd.DataFrame(lista_guias, columns=['Guias'])

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

    sheet.batch_clear(["A2:Z100000"])

    data = df_itens_faltantes.values.tolist()
    sheet.update('A2', data)

st.set_page_config(layout='wide')

st.session_state.id_gsheet = '1GR7c8KvBtemUEAzZag742wJ4vc5Yb4IjaON_PL9mp9E'

# Título da página

st.title('Mapa de Pagamento - Guias (Histórico)')

st.divider()

row1 = st.columns(2)

# Objetos pra colher período do mapa

with row1[0]:

    container_datas = st.container(border=True)

    container_datas.subheader('Período')

    data_inicial = container_datas.date_input('Data Inicial', value=None ,format='DD/MM/YYYY', key='data_inicial')

    data_final = container_datas.date_input('Data Final', value=None ,format='DD/MM/YYYY', key='data_final')

st.divider()

# Script pra gerar mapa de pagamento

if data_final and data_inicial:

    row1_2=container_datas.columns(2)

    with row1_2[0]:

        gerar_mapa = st.button('Puxar Mapa de Pagamentos')

    if gerar_mapa:

        with st.spinner('Puxando mapas de pagamentos...'):

            puxar_aba_simples(st.session_state.id_gsheet, 'Histórico de Pagamentos', 'df_historico_pagamentos')

            st.session_state.df_historico_pagamentos['Data da Escala'] = pd.to_datetime(st.session_state.df_historico_pagamentos['Data da Escala'], format='%d/%m/%Y').dt.date

            for coluna in ['Valor', 'Acréscimo Motoguia', 'Desconto por Junção', 'Valor Total']:

                st.session_state.df_historico_pagamentos[coluna] = (st.session_state.df_historico_pagamentos[coluna].str.replace('.', '', regex=False).str.replace(',', '.', regex=False))

                st.session_state.df_historico_pagamentos[coluna] = pd.to_numeric(st.session_state.df_historico_pagamentos[coluna])

            st.session_state.df_pag_final_historico = st.session_state.df_historico_pagamentos[(st.session_state.df_historico_pagamentos['Data da Escala'] >= data_inicial) & 
                                                                                            (st.session_state.df_historico_pagamentos['Data da Escala'] <= data_final)].reset_index(drop=True)

if 'df_pag_final_historico' in st.session_state:

    st.header('Gerar Mapas')

    row2 = st.columns(2)

    with row2[0]:

        lista_guias = st.session_state.df_pag_final_historico[~(st.session_state.df_pag_final_historico['Guia'].isin(['SEM GUIA', 'NENHUM GUIA', ''])) & 
                                                              (~st.session_state.df_pag_final_historico['Guia'].str.contains('PDV')) & 
                                                              (~st.session_state.df_pag_final_historico['Guia'].str.contains('BASE AEROPORTO')) & 
                                                              (~st.session_state.df_pag_final_historico['Guia'].str.contains('VENDAS ONLINE'))]['Guia'].dropna().unique().tolist()

        guia = st.selectbox('Guia', sorted(lista_guias), index=None)

    if guia:

        row2_1 = st.columns(4)

        df_pag_guia = st.session_state.df_pag_final_historico[st.session_state.df_pag_final_historico['Guia']==guia].sort_values(by=['Data da Escala', 'Veículo', 'Motorista']).reset_index(drop=True)

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

                    df_pag_guia = st.session_state.df_pag_final_historico[st.session_state.df_pag_final_historico['Guia']==guia_ref].sort_values(by=['Data da Escala', 'Veículo', 'Motorista']).reset_index(drop=True)

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

                    df_pag_guia = st.session_state.df_pag_final_historico[st.session_state.df_pag_final_historico['Guia']==guia_ref].sort_values(by=['Data da Escala', 'Veículo', 'Motorista']).reset_index(drop=True)

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
