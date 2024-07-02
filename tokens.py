import os
import time
import json
import requests
import datetime
import pandas as pd

from logpy import uniao, tools as tl

# horario de parada diario (hora,minuto)
stop_time = (23,0)

# credenciais fixas
path_credenciais = os.path.join(uniao.getvault(),"API_Ampere","cred","userapi.txt")
credenciais = pd.read_csv(path_credenciais,header=None,sep=";")
user = credenciais.iloc[0,1]
hash_senha = credenciais.iloc[1,1]
user_token = credenciais.iloc[2,1]

while datetime.datetime.now() < datetime.datetime.now().replace(hour=stop_time[0], minute=stop_time[1]):
    db = tl.connection_db('AMPERE')
    
    # salva um novo 'x-access-token'
    link_login = 'https://exclusivo.ampereconsultoria.com.br/automated-login'
    headers = {
        'x-user-token': user_token,
        'Content-Type': 'application/json'
    }
    data = {
        'username': user,
        'password': hash_senha
    }
    response = requests.request('PUT',link_login,headers=headers,data=json.dumps(data)).json()
    
    # se o request deu certo, guarda no banco de dados
    x_access_token = 0
    if response['code'] == 200:
        x_access_token = response['data']['access_token']
        
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        validade = (datetime.datetime.now() + datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
        query = f'''
            INSERT INTO tokens (tipo,valor,hora_request,validade)
            VALUES 
            ('x-access-token','{x_access_token}','{now}','{validade}');
        '''
        db.query(query)
        db.db_commit()
        
    # pega um novo token para cada um dos produtos que olhamos
    produtos      = ['prevs-automatico','meteorologia','prevs-personalizado']
    responses     = []
    horas_request = []
    
    for produto in produtos:
        link_token = f'https://exclusivo.ampereconsultoria.com.br/admin/contratos/current-user-has-permission/?item={produto}'
        headers = {
            'x-access-token': x_access_token,
            'x-user-token': user_token
        }
        data = {
            'username': user,
            'password': hash_senha
        }
        response = requests.request('GET',link_token,headers=headers,data=json.dumps(data)).json()
        responses.append(response)
        horas_request.append(datetime.datetime.now())
    
    # se qualquer um der errado, recomeca
    if any([r['code'] != 200 for r in responses]):
        time.sleep(2*60)
        db.db_close()
        continue
    
    # guarda os tokens no bd
    query = '''
        INSERT INTO tokens (tipo,valor,hora_request,validade)
        VALUES
    '''
    for prod, resp, hora in zip(produtos,responses,horas_request):
        validade = hora + datetime.timedelta(hours=6)
        query += f'''
            ('{prod}','{resp['data']['product_key']}','{hora.strftime('%Y-%m-%d %H:%M:%S')}','{validade.strftime('%Y-%m-%d %H:%M:%S')}'),'''
    query = query.rstrip(',') + ';'
    
    db.query(query)
    db.db_commit()
    db.db_close()
    
    time.sleep(60*35)