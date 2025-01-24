import streamlit as st
import pandas as pd
import mysql.connector
import decimal
from babel.numbers import format_currency

def gerar_df_phoenix(vw_name, base_luck):

    config = {
    'user': 'user_automation_jpa',
    'password': 'luck_jpa_2024',
    'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com',
    'database': base_luck
    }
    conexao = mysql.connector.connect(**config)
    cursor = conexao.cursor()
    request_name = f'SELECT `Status da Reserva`, `Escala`, `Data da Escala`, `Veiculo`, `Tipo Veiculo`, `Servico`, `Tipo de Servico`, `Fornecedor Motorista`, `Motorista`, `Total ADT`, `Total CHD` FROM {vw_name}'
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

    st.session_state.view_phoenix = 'vw_pagamento_fornecedores_adicional'

    st.session_state.df_escalas = st.session_state.df_escalas_bruto[~(st.session_state.df_escalas_bruto['Status da Reserva'].isin(['CANCELADO', 'PENDENCIA DE IMPORTAÇÃO', 'RASCUNHO'])) & 
                                                                    ~(pd.isna(st.session_state.df_escalas_bruto['Status da Reserva'])) & ~(pd.isna(st.session_state.df_escalas_bruto['Escala']))]\
                                                                        .reset_index(drop=True)
    
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

        file.write(html)

        file.write(f'<br><br><p style="font-size:30px;">O valor total dos serviços é {soma_servicos}</p>')

st.set_page_config(layout='wide')

if not 'base_luck' in st.session_state:

    st.session_state.base_luck = 'test_phoenix_joao_pessoa'

if not 'df_escalas' in st.session_state or st.session_state.view_phoenix!='vw_pagamento_fornecedores_adicional':

    with st.spinner('Puxando dados do Phoenix...'):

        puxar_dados_phoenix()

st.title('Mapa de Pagamento - Balsa - João Pessoa')

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

    if atualizar_phoenix:

        with st.spinner('Puxando dados do Phoenix...'):

            puxar_dados_phoenix()

if gerar_mapa:

    df_escalas = st.session_state.df_escalas[(st.session_state.df_escalas['Data da Escala'] >= data_inicial) & (st.session_state.df_escalas['Data da Escala'] <= data_final)].reset_index(drop=True)

    df_escalas_group = df_escalas.groupby(['Data da Escala', 'Escala', 'Veiculo', 'Tipo Veiculo', 'Servico', 'Tipo de Servico', 'Fornecedor Motorista', 'Motorista'])\
            .agg({'Total ADT': 'sum', 'Total CHD': 'sum'}).reset_index()

    mask_balsa = ((df_escalas_group['Veiculo'].str.contains('MM0')) & (df_escalas_group['Servico']=='TRILHA DOS COQUEIRAIS')) | \
        ((df_escalas_group['Tipo Veiculo']=='Buggy') & (df_escalas_group['Servico'].str.contains('NORTE|COQUEIRAIS')))

    df_balsa = df_escalas_group[mask_balsa].reset_index(drop=True)

    df_balsa['Valor Balsa'] = df_balsa.apply(lambda row: 25.7 + (row['Total ADT'] + row['Total CHD'])*2 if row['Tipo Veiculo']!='Buggy' 
                                            else 19.2 + (row['Total ADT'] + row['Total CHD'])*2, axis=1)

    st.session_state.df_pag_final_forn = df_balsa[['Data da Escala', 'Servico', 'Fornecedor Motorista', 'Tipo Veiculo', 'Veiculo', 'Valor Balsa']]
    
if 'df_pag_final_forn' in st.session_state:

    df_pag_guia = st.session_state.df_pag_final_forn.sort_values(by=['Fornecedor Motorista', 'Data da Escala', 'Veiculo']).reset_index(drop=True)

    df_pag_guia['Data da Escala'] = pd.to_datetime(df_pag_guia['Data da Escala'])

    df_pag_guia['Data da Escala'] = df_pag_guia['Data da Escala'].dt.strftime('%d/%m/%Y')

    total_a_pagar = df_pag_guia['Valor Balsa'].sum()

    st.subheader(f'Valor Total: R${round(total_a_pagar, 2)}')

    container_dataframe = st.container()

    container_dataframe.dataframe(df_pag_guia, hide_index=True, use_container_width = True)

    soma_servicos = df_pag_guia['Valor Balsa'].sum()

    soma_servicos = format_currency(soma_servicos, 'BRL', locale='pt_BR')

    for item in ['Valor Balsa']:

        df_pag_guia[item] = df_pag_guia[item].apply(lambda x: format_currency(x, 'BRL', locale='pt_BR'))

    html = definir_html(df_pag_guia)

    nome_html = f"Balsa.html"

    nome_titulo_html = f"Balsa"

    criar_output_html(nome_html, html, nome_titulo_html, soma_servicos)

    with open(nome_html, "r", encoding="utf-8") as file:

        html_content = file.read()

    st.download_button(
        label="Baixar Arquivo HTML",
        data=html_content,
        file_name=nome_html,
        mime="text/html"
    )
