import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

schema = StructType([
    StructField("id", StringType(), True),
    StructField("mediaVotos", DoubleType(), True),
    StructField("popularidade", DoubleType(), True),
    StructField("orcamento", DecimalType(), True),
    StructField("receita", DecimalType(), True),
    StructField("produtorasCinema", StringType(), True),
    StructField("panoFundo", StringType(), True)  
])

df = spark.read.schema(schema) \
    .option("multiline", "true") \
    .json("s3://bucketchallenge7/Raw/Local/Tmdb/Json/2024/04/05/")
    
df = df.withColumn('lucro', (col('receita') - col('orcamento')).cast(DecimalType()))

# df.show(5)

df.write.parquet("s3://bucketchallenge7/TRT/Movies/movies_parquet_api")

job.commit()