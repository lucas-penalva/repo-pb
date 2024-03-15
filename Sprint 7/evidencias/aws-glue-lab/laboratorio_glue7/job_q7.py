import sys
# representa um conjunto de dados estruturados que pode ser transformado usando o Glue.
from awsglue.dynamicframe import DynamicFrame
# funções do Spark para realizar operações de soma e classificação decrescente em colunas
from pyspark.sql.functions import sum, desc
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

df = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [source_file]
    },
    "csv",
    {
        "withHeader": True,
        "separator": ","
    }
)

df = df.toDF()

# transformação: uma nova coluna "ano" é criada convertendo a coluna "ano" para o tipo  inteiro.
df = df.withColumn("ano", df["ano"].cast("int"))

# os dados são agrupados por "ano" e "sexo" e a soma dos "total" é calculada para cada grupo.
sum_by_year_sex = (
    df.groupBy("ano", "sexo")
      .agg(sum("total").alias("total_registros"))
      .orderBy("ano")
)

sum_by_year_sex.show(10)

job.commit()