""" script que baixa os mapas diarios que vao pro telegram
toda vez que roda isso, ele ve quais mapas ja baixamos, e tenta baixar os que faltam
a tarefa TELEGRAM do jarvis faz uso desses mapas
"""
import os
import time
import json
import base64
import requests
import datetime
import pandas as pd

from logpy import uniao, tools as tl

def baixar_comparativo(modelo,prev1,prev2,periodo,filenames,tokens):
    """ modelo = str
        filenames = [str, str, str]
        periodo = (datetime.date, datetime.date)
        prev2 > prev1  (datetime.date)
        tokens = {'x-access-token':valor,'meteorologia': valor}
    """
    # credenciais
    path_credenciais = os.path.join(uniao.getvault(),"API_Ampere","cred","userapi.txt")
    credenciais = pd.read_csv(path_credenciais,header=None,sep=";")
    user = credenciais.iloc[0,1]
    hash_senha = credenciais.iloc[1,1]
    user_token = credenciais.iloc[2,1]
    
    # prepara as datas pro formato da api
    prev1 = datetime.datetime(prev1.year,prev1.month,prev1.day,3,0,0)
    prev2 = datetime.datetime(prev2.year,prev2.month,prev2.day,3,0,0)
    periodo = [datetime.datetime(p.year,p.month,p.day,3,0,0) for p in periodo]
    
    # prepara o que vai mandar pra api
    link = f'https://exclusivo.ampereconsultoria.com.br/produtos/meteorologia/comparador-imagens-clima/?product_key={tokens["meteorologia"]}'
    headers = {
        'x-access-token': tokens['x-access-token'],
        'x-user-token': user_token
    }
    data = {
        'method': 'solicitar_comparacao',
        'params': {
            'tipo': 'comparacao',
            'comparacao': {
                'tipo': 1,
                'definir_periodo': False,
                'data_prev_base': prev1.timestamp(),
                'data_inicial': periodo[0].timestamp(),
                'data_final': periodo[1].timestamp(),
                'modelo_base': modelo,
                'rmv_base': True,
                'data_prev_confrontante': prev2.timestamp(),
                'modelo_confrontante': modelo,
                'rmv_confrontante': True
            }
        },
        'broadcast': False,
        'room': '',
        'user': 'exclusivo_comparador_client'           
    }
    
    # faz o request, se der certo salva as imagens (e salva o nome do arquivo no bd)
    response = requests.request("POST",link,headers=headers,data=json.dumps(data)).json()
    db = tl.connection_db('AMPERE')
    
    if response['code'] == 200:
        
        for fig, file in zip(['fig01','fig02','fig03'],filenames):
            if file is None:
                continue
            mapa64 = response["data"]["params"][fig][22:]
            # salvando arquivo
            with open(file,"wb") as f:
                f.write(base64.b64decode(mapa64))
            # salvando que arquivo existe no bd
            db.query(f"INSERT INTO mapas (arquivo) VALUES ('{str(file)}')")
            
    db.db_commit()
    db.db_close()

BAIXAR_MODELOS = ['nprevc','gefs','ecmwfens','ampere','gem']
LOCAL_DOWNLOAD = '/home/ubuntu/C:/Testando/ampere/mapas2'

if not os.path.exists(LOCAL_DOWNLOAD):
    os.mkdir(LOCAL_DOWNLOAD)


db = tl.connection_db('AMPERE')
info_modelos = pd.DataFrame(db.query('SELECT * FROM modelos')).set_index('nome')

for modelo in BAIXAR_MODELOS:
    
    horizonte = info_modelos.loc[modelo,'horizonte']
    
    qt_pentadas     = (horizonte // 5) if (horizonte % 5 < 3) else (horizonte // 5) + 1
    qt_comparativos = qt_pentadas - 1
    
    prev_hoje  = datetime.datetime.now().date()
    prev_ontem = prev_hoje - datetime.timedelta(days=1)
    
    # baixa tudo menos a ultima pentada (prev hoje), para cada modelo
    # faz isso com o minimo de downloads possiveis
    for n_pentada in range(qt_comparativos):
        
        arquivos = [
            os.path.join(LOCAL_DOWNLOAD,f'{modelo}_p{n_pentada+1}_prev_atual.png'),
            os.path.join(LOCAL_DOWNLOAD,f'{modelo}_p{n_pentada+1}_prev_ontem.png'),
            os.path.join(LOCAL_DOWNLOAD,f'{modelo}_comp_p{n_pentada+1}.png')
        ]
        
        inicio = prev_hoje + datetime.timedelta(days = 1 + n_pentada*5)
        fim    = inicio + datetime.timedelta(days = 4)
        
        for arquivo in arquivos:
            existe = db.query(f"SELECT hora_download FROM mapas WHERE arquivo = '{str(arquivo)}' ORDER BY hora_download DESC LIMIT 1")
            if existe == ():
                
                token_met = db.query("SELECT valor FROM tokens WHERE tipo = 'meteorologia' ORDER BY validade DESC LIMIT 1")[0]['valor']
                token_acesso = db.query("SELECT valor FROM tokens WHERE tipo = 'x-access-token' ORDER BY validade DESC LIMIT 1")[0]['valor']
                
                baixar_comparativo(modelo,prev_ontem,prev_hoje,[inicio,fim],arquivos,{'x-access-token':token_acesso,'meteorologia':token_met})
                
                break
        time.sleep(5)
    
    # baixa a ultima pentada (prev hoje)
    # eh a que nao vai pro comparativo
    arquivo = os.path.join(LOCAL_DOWNLOAD,f'{modelo}_p{qt_pentadas}_prev_atual.png')
    
    existe = db.query(f"SELECT hora_download FROM mapas WHERE arquivo = '{str(arquivo)}' ORDER BY hora_download DESC LIMIT 1")
    if existe == ():
        
        inicio = fim + datetime.timedelta(days=1)
        fim    = prev_hoje + datetime.timedelta(days=int(horizonte))
        
        token_met = db.query("SELECT valor FROM tokens WHERE tipo = 'meteorologia' ORDER BY validade DESC LIMIT 1")[0]['valor']
        token_acesso = db.query("SELECT valor FROM tokens WHERE tipo = 'x-access-token' ORDER BY validade DESC LIMIT 1")[0]['valor']
        
        baixar_comparativo(modelo,prev_hoje,prev_hoje,[inicio,fim],[arquivo,None,None],{'x-access-token':token_acesso,'meteorologia':token_met})
        time.sleep(5)

db.db_close()