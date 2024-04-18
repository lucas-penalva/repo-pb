import json
import os 
import requests as rqt
from datetime import datetime
import time
import boto3

def aws_credenciais():
    """Obtém as credenciais da AWS."""
    aws_access_key = os.environ["aws_access_key_id"] 
    aws_secret_access_key = os.environ['aws_secret_access_key']
    aws_session_token = os.environ['aws_session_token']
    return aws_access_key, aws_secret_access_key, aws_session_token

def obter_filmes(api_key, pagina):
    url = f"https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&language=en-US&offset=20&page={pagina}& \
    primary_release_date.gte=1990-01-01&primary_release_date.lte=2020-12-31& \
    sort_by=revenue.desc&with_genres=12%20and%2028&api_key={api_key}"
    resposta = rqt.get(url).json()
    resultados = resposta['results']
    return resultados

def obter_detalhes_filme(api_key, id_filme):
    url = f"https://api.themoviedb.org/3/movie/{id_filme}?api_key={api_key}&language=en-US"
    resposta = rqt.get(url).json()
    return resposta

def buscar_filmes(api_key, num_paginas):
    todos_filmes = []
    for pagina in range(1, num_paginas + 1):
        filmes_na_pagina = obter_filmes(api_key, pagina)
        todos_filmes.extend(filmes_na_pagina)
        time.sleep(2)
    return todos_filmes

def buscar_todos_detalhes_filmes(api_key, filmes):
    detalhes_filmes = []
    for filme in filmes:
        id_filme = filme['id']
        info_filme = obter_detalhes_filme(api_key, id_filme)
        detalhes_filmes.append(info_filme)
    return detalhes_filmes

def extrair_info(detalhes_filmes):
    """Extrai as informações relevantes dos filmes."""
    if 'id' in detalhes_filmes:
        companies = ', '.join([company['name'] for company in detalhes_filmes['production_companies']])
        genres = ', '.join([gen['name'] for gen in detalhes_filmes['genres']])
        
        movie_info = {
            'id': detalhes_filmes['imdb_id'],
            'tituloOriginal': detalhes_filmes['original_title'],
            'anoLancamento': detalhes_filmes['release_date'],
            'genero': genres,
            'popularidade': detalhes_filmes['popularity'],
            'orcamento': detalhes_filmes['budget'],
            'receita': detalhes_filmes['revenue'],
            'numeroVotos': detalhes_filmes['vote_count'],
            'mediaVotos': detalhes_filmes['vote_average'],
            'produtorasCinema': companies,
            'panoFundo': detalhes_filmes['backdrop_path']
        }
        return movie_info
    else:
        return None    

def lambda_handler(event, context):    
    aws_access_key_id, aws_secret_access_key, aws_session_token = aws_credenciais()
    
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token
    )
    
    s3_client = session.client('s3')
    
    api_key = os.environ['api_key']
    nome_bucket = 'bucketchallenge7'
    num_paginas = 25

    todos_filmes = buscar_filmes(api_key, num_paginas)
    filmes_com_detalhes = buscar_todos_detalhes_filmes(api_key, todos_filmes)
    
    data_movies = []
    
    for movie_data in filmes_com_detalhes:
        movie_info = extrair_info(movie_data)
        if movie_info:
            data_movies.append(movie_info)
    
    # Carregar os dados em lotes para o S3
    for i in range(0, len(data_movies), 100):
        dados_filmes_lote = data_movies[i:i+100]
        lote = json.dumps(dados_filmes_lote)
        data_atual = datetime.now().strftime('%Y/%m/%d')
        caminho_arquivo = f'Raw/Local/Tmdb/Json/{data_atual}/batch{i//100}.json'
        s3_client.put_object(Bucket=nome_bucket, Key=caminho_arquivo, Body=lote)

    return {
        'statusCode': 200,
        'body': ('Arquivos carregados com sucesso para o S3!')
    }