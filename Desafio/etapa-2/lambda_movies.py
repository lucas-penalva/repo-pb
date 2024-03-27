import boto3
import json
import os 
import requests as rqt
from datetime import datetime

def aws_credenciais():
    """Obtém as credenciais da AWS."""
    aws_access_key = os.environ["aws_access_key_id"] 
    aws_secret_access_key = os.environ['aws_secret_access_key']
    aws_session_token = os.environ['aws_session_token']
    return aws_access_key, aws_secret_access_key, aws_session_token

def buscar_filmes(movie_id, api_key):
    """Obtém os dados do filme da API."""
    url = f'https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key}'
    response = rqt.get(url)
    data = response.json()
    return data

def extrair_info(data):
    """Extrai as informações relevantes dos filmes."""
    if 'id' in data:
        companies = ', '.join([company['name'] for company in data['production_companies']])
        gen = ', '.join([gens['name'] for gens in data['genres']])
        
        movie_info = {
            'id': data['imdb_id'],
            'tituloOriginal': data['original_title'],
            'anoLancamento': data['release_date'],
            'genero': gen,
            'popularidade': data['popularity'],
            'orcamento': data['budget'],
            'receita': data['revenue'],
            'numeroVotos': data['vote_count'],
            'mediaVotos': data['vote_average'],
            'produtorasCinema': companies,
            'panoFundo': data['backdrop_path']
        }
        return movie_info
    else:
        return None

def upload_to_s3(data, bucket_name, file_path, aws_access_key, aws_secret_access_key, aws_session_token):
    """Faz o upload dos dados para o S3."""
    s3_client = boto3.client('s3', 
                             aws_access_key_id=aws_access_key, 
                             aws_secret_access_key=aws_secret_access_key, 
                             aws_session_token=aws_session_token)
    batch_data = json.dumps(data)
    s3_client.put_object(Bucket=bucket_name, Key=file_path, Body=batch_data)

def lambda_handler(event, context):
    aws_access_key, aws_secret_access_key, aws_session_token = aws_credenciais()
    api_key = os.environ['api_key']
    bucket_name = 'bucketchallenge8'
    ids = ['tt0258463', 'tt0372183', 'tt0440963', 'tt1194173', 'tt4196776', 'tt0083944',
           'tt0089880', 'tt0095956', 'tt0462499', 'tt1206885']
    
    data_movies = []

    for movie_id in ids:
        movie_data = buscar_filmes(movie_id, api_key)
        movie_info = extrair_info(movie_data)
        if movie_info:
            data_movies.append(movie_info)

    data_atual = datetime.now().strftime('%Y/%m/%d')
    file_path = f'Raw/Local/Tmdb/Json/{data_atual}/batch.json'
    upload_to_s3(data_movies, bucket_name, file_path, aws_access_key, aws_secret_access_key, aws_session_token)

    return {
        'statusCode': 200,
        'body': ('Arquivo carregado com sucesso para o S3!')
    }
