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

## Desafio - Parte III

***_I. Tarefa 03_***

Nesta fase, iniciamos realizando a integração das diversas fontes de origem.

Separamos o processamento em dois jobs: o [primeiro](<./etapa-3/tarefa_03/trusted_job_csv.py>), para carga histórica, responsável pelo processamento dos arquivos CSV  e o [segundo](<./etapa-3/tarefa_03/trusted_job_api.py>), para carga de dados do TMDB.

***_II. Tarefa 04_***

Desenvolvemos a modelagem de dados da Refined.

 [Visualizar modelagem](<./etapa-3/tarefa_04/modelagem_multidim.png>)

***_III. Tarefa 05_***

Criamos o processamento que lê os dados existentes na Trusted, processamos e inserimos na [Refined](<./etapa-3/tarefa_05/refined_job.py>).

Por fim, foram registradas as [tabelas](<./etapa-3/tarefa_05/tables_athena.jpg>) geradas no AWS Glue Data Catalog para poder fazer consultas.

## Desafio - Parte IV

**H1. A variação na média de orçamento em um período de três décadas aumentou ou diminuiu e por quê? Quais fatores influenciam?**

![H1](../Sprint%2010/images/h1.jpg)

**H2. A variação na média de receita em um período de três décadas aumentou ou diminuiu e por quê?**

![H2](../Sprint%2010/images/h2.jpg)

**H3. Demonstrar o comportamento da média de lucro por gênero**

![H3](../Sprint%2010/images/h3.jpg)

## Conclusões:

**H1.** Nota-se uma variação na média de orçamento num período de 30 anos.

Isso explica o fato de que houve avanços tecnológicos, cinematográficos, equipamentos, que exigem investimentos mais altos para alcançar um nível de qualidade e isso reflete em um filme competitivo no mercado.

**H2.**

Nota-se uma tendência na variação média de receita num período de 30 anos, com quedas acentuadas e logo em seguida uma recuperação.

Esse gráfico demonstra 2 pontos importantes:

1. No ano de 2008, com a crise do Subprime, houve redução de orçamentos para a produção de filmes e isso fez com que investidores ficassem mais cautelosos. Não somente, o consumidor mudou seus hábitos de entretenimento, ou seja, menos gasto com cinema e produtos relacionados.

2. No ano de 2020, nota-se pelo gráfico uma queda forte devido ao fechamento de cinemas a nível mundial pois, as restrições de saúde recomendadas impactaram diretamente na receita de bilheteria, fazendo com que resultasse em atrasos de lançamentos, cancelamentos e redução na produção de novos filmes.

Além disso, houve uma forte mudança para o serviço de Streaming, e isso fez com que os estúdios avaliassem suas estratégias para descobrir novas formas de distribuir suas produções. 

Por fim, o aumento de custos durante a produção, aconteceu devido a medidas de segurança e protocolos de saúde, resultando em custos adicionais.

**H3.**

Nesse gráfico eu quis apresentar a média de lucro por gênero pra que o investidor possa tomar uma decisão ou não se vale a pena investir em filmes de determinado gênero. Com isso, por meio dessa visualização ele consegue obter uma visão de um possível retorno de lucro e tomar decisões mais assertivas.

