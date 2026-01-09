import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector

yourbucketname = 'dea-course-wayde'
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1717349525887 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://" + yourbucketname + "/northwind-dataset/customers.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1717349525887")

# Script generated for node Detect Sensitive Data
detection_parameters = {"PHONE_NUMBER": [{
  "action": "REDACT",
  "actionOptions": {"redactText": "******"}
}]}

entity_detector = EntityDetector()
DetectSensitiveData_node1717349938067 = entity_detector.detect(AmazonS3_node1717349525887, detection_parameters, "DetectedEntities", "HIGH")

# Script generated for node Amazon S3
AmazonS3_node1717350372831 = glueContext.write_dynamic_frame.from_options(frame=DetectSensitiveData_node1717349938067, connection_type="s3", format="glueparquet", connection_options={"path": "s3://" + yourbucketname + "/northwind-dataset/cleaned/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1717350372831")

job.commit()