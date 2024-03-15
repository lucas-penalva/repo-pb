import sys
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, upper

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','S3_INPUT_PATH','S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

# Cria o DataFrame a partir dos dados de entrada
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [source_file]},
    format="csv",
    format_options={"withHeader": True, "separator": ","}
)

# Converte de DynamicFrame para DataFrame
df = dynamic_frame.toDF()

# Transforma os valores da coluna "nome" para maiúsculo
df = df.withColumn("nome", upper(col("nome")))

# Escreve o DataFrame particionado por sexo e ano no S3 no formato JSON
df.write.partitionBy("sexo", "ano").mode("overwrite").json(target_path)

job.commit()

"""O modo de escrita como "overwrite", significa que se houver
 algum diretório existente no local de armazenamento de destino com o mesmo caminho especificado,
 ele será substituído pelos novos dados. Isso serve para quando a gente quer substituir os dados existentes com os dados atualizados."""

