import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("delimiter", "|")\
    .load("s3://bucketchallenge7/bucketchallenge7/Raw/Local/CSV/Movies/2024/03/11/")

df_filtered = df.filter(
    (col("genero").contains('Action') | col("genero").contains('Adventure')) &
    (col("notaMedia") >= 6.5) &
    (col("anoLancamento").between(1990, 2020))
    ).select('id', 'tituloOriginal', 'anoLancamento', 'genero', 'notaMedia', 'numeroVotos', 'nomeArtista')    
    
# df_filtered.show(5)

df_filtered.write.parquet("s3://bucketchallenge7/TRT/Movies/movies_parquet")

job.commit()