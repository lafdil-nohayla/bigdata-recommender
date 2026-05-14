import os, json, logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

spark = SparkSession.builder     .appName('ALS-Light')     .config('spark.sql.shuffle.partitions', '8')     .config('spark.driver.memory', '2g')     .config('spark.executor.memory', '1g')     .config('spark.memory.fraction', '0.6')     .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

log.info('Chargement dataset...')
df = spark.read.csv('/opt/spark-data/Reviews.csv', header=True, inferSchema=False)     .select('UserId','ProductId','Score').dropna().limit(50000).cache()
log.info('Lignes: %d', df.count())

pipe = Pipeline(stages=[
    StringIndexer(inputCol='UserId',    outputCol='user_idx',    handleInvalid='skip'),
    StringIndexer(inputCol='ProductId', outputCol='product_idx', handleInvalid='skip'),
])
idx = pipe.fit(df)
df2 = idx.transform(df)     .withColumn('user_idx',    F.col('user_idx').cast(IntegerType()))     .withColumn('product_idx', F.col('product_idx').cast(IntegerType()))     .withColumn('score',       F.col('Score').cast(FloatType()))     .select('user_idx','product_idx','score').cache()

train, val, test = df2.randomSplit([0.8,0.1,0.1], seed=42)
train = train.cache()
log.info('Train=%d Test=%d', train.count(), test.count())

als = ALS(rank=8, maxIter=5, regParam=0.1,
          userCol='user_idx', itemCol='product_idx', ratingCol='score',
          coldStartStrategy='drop', nonnegative=True)

log.info('Entrainement ALS...')
model = als.fit(train)

preds = model.transform(test).dropna(subset=['prediction'])
rmse  = RegressionEvaluator(metricName='rmse',labelCol='score',predictionCol='prediction').evaluate(preds)
log.info('RMSE: %.4f', rmse)

os.makedirs('/opt/spark-models', exist_ok=True)
model.save('/opt/spark-models/als_model')
idx.save('/opt/spark-models/als_model_indexer')

with open('/opt/spark-models/report.json','w') as f:
    json.dump({'rmse':round(rmse,4),'rank':8,'maxIter':5,'regParam':0.1,'sample_size':50000,'train_count':train.count(),'test_count':test.count()},f,indent=2)
log.info('Modele sauvegarde!')
spark.stop()
