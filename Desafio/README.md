## Desafio - Parte I

***Contexto: Criar código Python que carrega arquivos CSV para a Nuvem utilizando as técnicas de ETL.***

A ingestão batch é o processo de coletar, processar e carregar grandes volumes de dados de uma vez só, em lotes. É usado em sistemas de armazenamento de dados para processar grandes quantidades de informações, evitando sobrecarga.

Nesta primeira parte do desafio, criamos um container docker com um volume para armazenar arquivos no bucket. Após isso, executamos localmente o container para realizar o upload dos dados para um bucket criado no S3.

* [Clique aqui para visualizar o código desenvolvido em Python](<./etapa-1/s3_files.py>)

* [Clique aqui para visualizar o código Dockerfile](<./etapa-1/Dockerfile>)

![Confira](/Desafio/etapa-1/docker-commands.jpg)

<!--2....
[Etapa II](etapa-2/entrega.txt)-->