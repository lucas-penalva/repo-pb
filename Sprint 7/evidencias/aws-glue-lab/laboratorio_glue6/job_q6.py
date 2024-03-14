import sys
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import sum, desc
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME, S3_INPUT_PATH, S3_TARGET_PATH]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

# Carregando os dados
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
).toDF()

# Convertendo o campo 'ano' para inteiro
df = df.withColumn("ano", df["ano"].cast("int"))

# Agrupando por nome, sexo e ano e somando os registros
sum_by_name_sex = (
    df.groupBy("nome", "sexo", "ano")
      .agg(sum("total").alias("total_registros"))
)

# Filtrando apenas os registros masculinos e encontrando o nome com mais registros em um determinado ano
max_male_record = (
    sum_by_name_sex.filter(sum_by_name_sex.sexo == "M")
                   .orderBy(desc("total_registros"))
                   .select("nome", "ano", "total_registros")
                   .first()
)

# Extraindo os valores
max_male_name = max_male_record["nome"]
max_male_year = max_male_record["ano"]
max_male_total = max_male_record["total_registros"]

print(f"Nome masculino com mais registros: {max_male_name} com {max_male_total} registros em {max_male_year}")

job.commit()