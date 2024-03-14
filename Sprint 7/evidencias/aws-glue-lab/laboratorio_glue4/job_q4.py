import sys
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
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

# Lê o arquivo CSV e cria um DataFrame
df = glueContext.create_dynamic_frame.from_options(
    "s3",
    {"paths": [source_file]},
    "csv",
    {"withHeader": True, "separator": ","}  # Supondo que o separador seja vírgula
)

# Converte o DataFrame para um DataFrame do Spark
df_spark = df.toDF()

# Agrupa os dados pelas colunas 'ano' e 'sexo' e conta o número de registros em cada grupo
contagem_nomes = df_spark.groupBy("ano", "sexo").count()

# Ordena os dados de modo que o ano mais recente apareça como o primeiro registro
contagem_nomes = contagem_nomes.orderBy(contagem_nomes['ano'].desc())

# Imprime o DataFrame resultante
contagem_nomes.show()

dynamic_frame_names = DynamicFrame.fromDF(contagem_nomes, glueContext, "dynamic_frame_names")

# Escreve o DataFrame resultante em formato Parquet no caminho de destino
glueContext.write_dynamic_frame.from_options(
    frame=contagem_nomes,
    connection_type="s3",
    connection_options={"path": target_path},
    format="csv"
)

job.commit()