## Desafio - Parte I

***Contexto: Criar código Python que carrega arquivos CSV para a Nuvem utilizando as técnicas de ETL.***

A ingestão batch é o processo de coletar, processar e carregar grandes volumes de dados de uma vez só, em lotes. É usado em sistemas de armazenamento de dados para processar grandes quantidades de informações, evitando sobrecarga.

Nesta primeira parte do desafio, criamos um container docker com um volume para armazenar arquivos no bucket. Após isso, executamos localmente o container para realizar o upload dos dados para um bucket criado no S3.

* [Clique aqui para visualizar o código desenvolvido em Python](<./etapa-1/s3_files.py>)

* [Clique aqui para visualizar o código Dockerfile](<./etapa-1/Dockerfile>)

![Confira](/Desafio/etapa-1/docker-commands.jpg)

## Desafio - Parte II

![tmdb](/Desafio/etapa-2/tmdb.jpg)

## Contexto do projeto:

Para este projeto, acerca do gênero ***Ação/Aventura***, escolhi analisar a saga de filmes ***_Jason Bourne_***, extraindo as informações necessárias e complementares via API para responder as hipóteses de negócio.

_Por que da análise?_ 

A intenção é divertir com os dados, cruzar as informações e realizar as análises para conclusão do projeto final.

## Estratégias:

_Etapa I. Entender o problema, identificando a análise que gostaria de realizar._

_Etapa II. Buscar os dados que julgo necessário, por meio do processo de ETL, fazendo as requisições pelo site do <a href="https://developer.themoviedb.org/reference/intro/getting-started" target="_blank">TMDB.</a> Três API's foram utilizadas para chegar no resultado desejado com os seguintes ***endpoints***:_

* https://api.themoviedb.org/3/search/movie
* https://api.themoviedb.org/3/movie/{movie_id}/external_ids
* https://api.themoviedb.org/3/movie/{movie_id}

_Etapa III Levantar hipóteses, gerar insights relevantes e validá-los nas próximas etapas._

_Etapa IV. Desenvolver o código localmente com as informações/dados obtidos(as) pela API, realizando os testes e posteriormente aplicar o mesmo no serviço <a href="https://aws.amazon.com/pt/lambda/" target="_blank">AWS Lambda</a> e enviar o arquivo gerado com os dados para um bucket no <a href="https://aws.amazon.com/pt/s3/" target="_blank">S3</a>, processo conhecido como ingestão batch._

## Premissas:

Adotar a métrica de _popularidade_ para extrair diversas informações.

## Insights:

1. Qual média de faturamento dos filmes da saga?

2. Qual a média de orçamento dos filmes da saga?

3. Houve variação do faturamento no ano de lançamento entre um filme e outro?

4. O filme que obteve o maior faturamento, teve um maior ou menor orçamento em relação ao que teve maior orçamento? Qual foi a diferença em percentual?

5. Existe correlação entre a popularidade e o sucesso de um filme? Caso sim, qual filme teve maior e menor popularidade? Qual foi o lucro de ambos?

6. Existe correlação entre o ano de lançamento de um filme e a sua popularidade?

## Resultados:

[Clique aqui](<./etapa-2/lambda_movies.py>) para visualizar o código lambda desenvolvido na etapa IV, e o resultado final do [arquivo](<./etapa-2/output_batch.jpg>) gerado e carregado para o bucket.