from pyspark.sql import SparkSession # type: ignore
from collections import namedtuple
import sys

# Configuration for Azure storage access
AzureContainer = namedtuple('AzureContainer', ['accountName', 'accountKey', 'name'])

def create_spark_session(container: AzureContainer) -> SparkSession:
    """ Initialize and return a Spark session with Azure configuration. """
    spark = SparkSession.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    spark.conf.set(f'fs.azure.account.key.{container.accountName}.dfs.core.windows.net', container.accountKey)        
    return spark

def setup_data_sources(spark: SparkSession, container: AzureContainer, delta_home: str):
    """ Create delta tables for hotel weather and expedia data. """
    paths = {
        "hotel_weather": "hotel-weather",
        "expedia": "expedia"
    }
    for table, folder in paths.items():
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS delta.`{delta_home}/{table}`
            USING delta
            AS SELECT * FROM parquet.`{get_container_path(container)}/{folder}`
        """)

def get_container_path(container: AzureContainer) -> str:
    """ Return the full Azure blob file system path for a container. """
    return f'abfss://{container.name}@{container.accountName}.dfs.core.windows.net'

def run_queries(spark: SparkSession, delta_home: str):
    """ Run analytical queries and save results to delta format. """
    queries = {
        "max_tmpr_diff_hotels": ('''
            SELECT id, ANY_VALUE(name) AS name, ANY_VALUE(address) AS address, ANY_VALUE(city) AS city,
                   ANY_VALUE(country) AS country, DATE_FORMAT(ANY_VALUE(wthr_date), 'yyyy-MM') AS wthr_month,
                   MAX(avg_tmpr_c) - MIN(avg_tmpr_c) AS tmpr_diff_c,
                   ANY_VALUE(year) AS year, ANY_VALUE(month) AS month
            FROM delta.`{}/hotel_weather`
            GROUP BY id, TRUNC(wthr_date, 'MM')
            ORDER BY tmpr_diff_c DESC
            LIMIT 10
        ''', "Task #1:"),
        "busy_hotels": ('''
            SELECT DISTINCT hotel_id, name, address, city, country, DATE_FORMAT(srch_date, 'yyyy-MM') AS srch_month,
                            srch_count, YEAR(srch_date) AS year, MONTH(srch_date) AS month
            FROM (
                SELECT hotel_id, srch_date, COUNT(srch_date) AS srch_count,
                       ROW_NUMBER() OVER (PARTITION BY srch_date ORDER BY COUNT(srch_date) DESC) AS rn
                FROM (
                    SELECT hotel_id, EXPLODE(SEQUENCE(TRUNC(srch_ci, 'MM'), TRUNC(srch_co, 'MM'), INTERVAL 1 MONTH)) AS srch_date
                    FROM delta.`{}/expedia`
                    WHERE srch_ci <= srch_co
                )
                GROUP BY hotel_id, srch_date
            ) visits
            JOIN delta.`{}/hotel_weather` hw ON visits.hotel_id = hw.id
            WHERE rn <= 10
            ORDER BY srch_month, srch_count DESC
        ''', "Task #2:")
    }

    for name, (query, label) in queries.items():
        print(label)
        result = spark.sql(query.format(delta_home))
        result.show(truncate=False)
        result.write.format('delta').partitionBy('year', 'month').mode('overwrite').save(f'{delta_home}/{name}')

def run():
    args = sys.argv[1:]
    container_info = AzureContainer(*args[:3])
    delta_home = args[3]

    spark = create_spark_session(container_info)
    setup_data_sources(spark, container_info, delta_home)
    run_queries(spark, delta_home)

if __name__ == '__main__':
    run()
