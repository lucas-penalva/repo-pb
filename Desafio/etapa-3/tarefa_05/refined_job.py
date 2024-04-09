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
                                     'S3_TARGET_MOVIES', 'S3_TARGET_REVIEWS',
                                     'S3_TARGET_FINANCES', 'S3_TARGET_EXTRAS'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df_movies = args['S3_INPUT_MOVIES']
df_api = args['S3_INPUT_API']
df_films = args['S3_TARGET_MOVIES']
df_reviews = args['S3_TARGET_REVIEWS']
df_finances = args['S3_TARGET_FINANCES']
df_extras = args['S3_TARGET_EXTRAS']

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

movies = spark.sql("""
              select
                    m.idMovies,
                    m.tituloOriginal,
                    m.genero,
                    m.anoLancamento,
                    m.nomeArtista,
                    m.panoFundo
              from movies m
""")

reviews = spark.sql("""
              select
                    m.idMovies,
                    m.notaMedia,
                    m.mediaVotos,
                    m.numeroVotos,
                    m.popularidade
              from movies m
""")

finances = spark.sql("""
              select
                    m.idMovies,
                    m.orcamento,
                    m.receita,
                    m.lucro
              from movies m
""")

extras = spark.sql("""
              select
                    m.idMovies,
                    m.produtorasCinema
              from movies m
""")

movies.write.format('parquet').mode('overwrite').option('path', df_films).saveAsTable('dbmovies.tb_movies')
reviews.write.format('parquet').mode('overwrite').option('path', df_reviews).saveAsTable('dbmovies.tb_reviews')
finances.write.format('parquet').mode('overwrite').option('path', df_finances).saveAsTable('dbmovies.tb_finances')
extras.write.format('parquet').mode('overwrite').option('path', df_extras).saveAsTable('dbmovies.tb_extras')

job.commit()