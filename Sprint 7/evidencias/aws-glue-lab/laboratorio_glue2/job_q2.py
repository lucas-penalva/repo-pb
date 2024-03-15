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

# Crie o DataFrame a partir dos dados de entrada
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [source_file]},
    format="csv",
    format_options={"withHeader": True, "separator": ","}
)

# Converte DynamicFrame para DataFrame
df = dynamic_frame.toDF()

df_upper = df.withColumn("nome", upper("nome"))

df_upper.show()

# Converte o DataFrame de volta para DynamicFrame
dynamic_frame_upper = DynamicFrame.fromDF(df_upper, glueContext, "dynamic_frame_upper")

# Escreve o DataFrame para o S3
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_upper,
    connection_type="s3",
    connection_options={"path": target_path},
    format="csv"
)

job.commit()