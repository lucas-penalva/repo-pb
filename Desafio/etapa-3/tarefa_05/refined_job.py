import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, coalesce
from pyspark.sql.types import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_MOVIES', 'S3_INPUT_API',
                                     'S3_TARGET_MOVIES', 'S3_TARGET_FATO'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df_movies = args['S3_INPUT_MOVIES']
df_api = args['S3_INPUT_API']
df_fato = args['S3_TARGET_FATO']
df_films = args['S3_TARGET_MOVIES']

# Reading parquet files
df1 = spark.read.parquet(df_movies)
df2 = spark.read.parquet(df_api)

# Dataframe joins
df_joined = df1.join(df2, df1['id'] == df2['id'], 'left')

# Select Columns
df_full = df_joined.select(
    coalesce(df1['id'], df2['id']).alias('idMovies'),
    col('tituloOriginal'),
    col('genero'),
    col('anoLancamento'),
    col('notaMedia'),
    col('mediaVotos'),
    col('numeroVotos'),
    col('popularidade'),
    col('nomeArtista'),
    col('orcamento'),
    col('receita'),
    col('lucro'),
    col('produtorasCinema'),
    col('panoFundo')
)

# df_full.show(10)

df_full.createOrReplaceTempView("movies")

fato = spark.sql("""
              select
                    m.idMovies,
                    m.nomeArtista,
                    m.notaMedia,
                    m.mediaVotos,
                    m.numeroVotos,
                    m.popularidade,
                    m.orcamento,
                    m.receita,
                    m.lucro
              from movies m
""")

movies = spark.sql("""
              select
                    m.idMovies,
                    m.tituloOriginal,
                    m.genero,
                    m.anoLancamento,
                    m.produtorasCinema,
                    m.panoFundo
              from movies m
""")

fato.write.format('parquet').mode('overwrite').option('path', df_fato).saveAsTable('dbmovies.fato_movies')
movies.write.format('parquet').mode('overwrite').option('path', df_films).saveAsTable('dbmovies.tb_movies')

job.commit()