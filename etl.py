import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())

mstr = glueContext.create_dynamic_frame.from_catalog(
             database="nike",
             table_name="xtra_mst")
print "Count: ", mstr.count()
mstr.printSchema()

sold = glueContext.create_dynamic_frame.from_catalog(
                 database="nike",
                 table_name="xstr_sold")
print "Count: ", sold.count()

sold = sold.concat(sold.col('ms_loc_x'),data.lit(','), sf.col('xs_loc_y'))

sold.printSchema()

data = Join.apply(mstr, sold, 'xm_ref', 'xm_key').drop_fields(['xm_stock', 'xm_key', 'xm_stock', ])
print "Count: ", data.count()
data.printSchema()

data = data.rename_field('xs_date', 'xm_name', 'xs_quantity').rename_field(
                               'sold_date', 'sold_product', 'sold_quantity')



for df_name in data.keys():
    m_df = data.select(df_name)
    print "Writing to Redshift table: ", df_name
    glueContext.write_dynamic_frame.from_jdbc_conf(frame = m_df,
                                                   catalog_connection = "redshift3",
                                                   connection_options = {"dbtable": xtra_data, "database": "nike"},
                                                   redshift_tmp_dir = "s3://nike/test/")
