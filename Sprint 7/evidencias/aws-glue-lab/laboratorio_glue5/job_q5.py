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

# Filtrando apenas os registros femininos e encontrando o nome com mais registros em um determinado ano
max_female_record = (
    sum_by_name_sex.filter(sum_by_name_sex.sexo == "F")
                   .orderBy(desc("total_registros"))
                   .select("nome", "ano", "total_registros")
                   .first()
)

# Extraindo os valores
max_female_name = max_female_record["nome"]
max_female_year = max_female_record["ano"]
max_female_total = max_female_record["total_registros"]

print(f"Nome feminino com mais registros: {max_female_name} com {max_female_total} registros em {max_female_year}")

job.commit()