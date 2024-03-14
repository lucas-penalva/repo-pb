# funções para interagir com o sistema operacional
import os
# carregamento dos dados para o S3
import boto3
import csv
from datetime import datetime

AWS_REGION = 'us-east-1'
BUCKET_NAME = 'bucketchallenge7'

# A função utiliza boto3 que é uma lib para interagir com serviços da AWS, e ela utiliza as credenciais fornecidas.
def upload_to_s3(file_path, raw, local, csv, data_specification, processing_date):
    """
    Carrega um arquivo para o S3 seguindo o padrão especificado.    """
    
    s3 = boto3.client('s3', region_name='us-east-1', aws_access_key_id = 'ASIAU6GD2AF24F7DS35D',
    aws_secret_access_key = 'aPfxRRdKJ+htZEf1uq6rCxUAihbB3iyXK73F1lZn', aws_session_token = 'IQoJb3JpZ2luX2VjECwaCXVzLWVhc3QtMSJHMEUCIGZ+hu9uZJiGiu/SGXy4hBA2A0EPVM1fOVOk3hff3wQEAiEA5rBNVkGOIRa1ZXfk0JXluI6hbuRdybJ5rKZFquVlSeEqnQMINBAAGgwzMzk3MTMwNjUzMzMiDK7wD7V+gfDRqSKZ5ir6AtR3PatWy2XOt2xssMTQ/wwthYGRAOZEoaiDCLCl0tF47kE84oBaymh6SXNcJ+mC1l83h+7Bg6ZiLL9oabx3CxrGo2GaxR062crVjhfYkLto/CCSWXNl4hOUBMbe4hj2Yd+MFGlGqeors36h0CV5sidW/567JungG/Y9n3A2lY5nJM1evZJPJFFeRZhHIw18O8bEca1GgTlf0zGxuqfU/7xETOkgewLBjrGeK8W0X7BvaG/Ktrw9y51QryXIljwMKleXlzyNgKWXpHzOrUP0ANQOdMuv492mRbDtDuJLR9BfyGRu79qgJS6rzrLqZXLXRxzxWJX6OvMTFSK8VZBXOSVGLRvw/fVggZmNH2ANtIbnw5jDAjoHF8y0KdgPWcwMk9tQ/Pbs9qE/yBFF4TwYGJ62+dltW2NSwC1D532qQRgzJ1E2IKgXo7iqwQAbSzb7mEjX0fWgDI+R1Wo3kzfqfSVLHd2Np9xjoNklEpfERUjTS6jiVDPZ01lGjjDHr72vBjqmATmVxD9fk867RkaWtkIWkw5moVvoNMItv/EZL4OvL7CaL+deEU1I50MAKbGKI8u6p5o0uUfi6EF7uD3mPlsI0Eo6Zfx7X0ekuHt+ZwaFccSV4RlGahyX3GYrWS7DFDsSY9GOkjahAL9yqpVWo3WcAgzPJvZyFZGbDcMmw6Kyn5wS1ac/lkAFsz3+nRGAsIsWeyNf7ZvSG9k0BBvEVMIXV5lZiBNvIdQ=')
    
    file_name = os.path.basename(file_path)
    # A variavel (key) contém uma estrutura (Raw Zone) que organiza de forma hierárquica os diretórios para facilitar a recuperação de dados
    key = f"{'bucketchallenge7'}/{raw}/{local}/{csv}/{data_specification}/{processing_date}/{file_name}"
 
    try:
        s3.upload_file(file_path, 'bucketchallenge7', key) # O arquivo é então carregado para o S3 usando o método upload_file do boto3
        print(f"Arquivo {file_path} enviado para o S3.")
    except Exception as e:
        print(f"Erro ao enviar o arquivo {file_path} para o S3: {e}")    
        
# Esta função é responsável por carregar os arquivos principais (filmes e séries) para o S3.
def main():
    # Ler e carregar o arquivo de filmes
    filmes_file_path = '/workspaces/repo-pb/movies.csv'
    upload_to_s3(filmes_file_path, 'Raw', 'Local', 'CSV', 'Movies', datetime.now().strftime('%Y/%m/%d'))

    # Ler e carregar o arquivo de séries
    series_file_path = '/workspaces/repo-pb/series.csv'
    upload_to_s3(series_file_path, 'Raw', 'Local', 'CSV', 'Series', datetime.now().strftime('%Y/%m/%d'))

if __name__ == "__main__":
    main()