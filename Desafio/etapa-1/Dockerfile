FROM python:3

# Instalar dependências do boto3
RUN pip install boto3

# Definir o diretório de trabalho
WORKDIR /workspaces/repo-pb

# Copiar os arquivos CSV para o diretório de trabalho
COPY movies.csv series.csv /workspaces/repo-pb/

# Copiar o script Python para o diretório de trabalho
COPY s3_files.py /workspaces/repo-pb

COPY ~/.aws/credentials /root/.aws/credentials

CMD ["python3", "s3_files.py"]