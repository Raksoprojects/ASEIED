# aws s3 cp s3://aseieddata/main.py .
# spark-submit main.py

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as func


if __name__ == "__main__":

    session = SparkSession.builder.appName("Taxi").getOrCreate()
    # sc = session.sparkContext
    # sc.setLogLevel('ERROR')
    dataFrameReader = session.read

    responses_g19 = dataFrameReader \
        .option("header", "true") \
        .option("inferSchema", value = True) \
        .csv("s3n://aseieddata/green_tripdata_2019-05.csv")

    responses_y19 = dataFrameReader \
        .option("header", "true") \
        .option("inferSchema", value = True) \
        .csv("s3n://aseieddata/yellow_tripdata_2019-05.csv")

    responses_g20 = dataFrameReader \
        .option("header", "true") \
        .option("inferSchema", value = True) \
        .csv("s3n://aseieddata/green_tripdata_2020-05.csv")

    responses_y20 = dataFrameReader \
        .option("header", "true") \
        .option("inferSchema", value = True) \
        .csv("s3n://aseieddata/yellow_tripdata_2020-05.csv")            

    '''
    Selecting all csv's.

    Then taking 3 responses from all of them.
    Calculating the average speed and saving it to csv.

    '''
    # ====================================================G19======================================
    print("=== Print Green taxis 2019 ===")
    

    g19_time = responses_g19.select((func.unix_timestamp("lpep_dropoff_datetime") - func.unix_timestamp("lpep_pickup_datetime")), "trip_distance")
    g19_time = g19_time.select(func.col("(unix_timestamp(lpep_dropoff_datetime, yyyy-MM-dd HH:mm:ss) - unix_timestamp(lpep_pickup_datetime, yyyy-MM-dd HH:mm:ss))").cast("int").alias("time"), func.col("trip_distance").cast("int").alias("trip_distance"))
    g19_avg = g19_time.select(func.avg((1000 * func.col("trip_distance")) / func.col("time")))
    
    g19_avg.show()

    g19 = g19_avg.head()[0]

    # ====================================================Y19======================================
    print("=== Print Yellow taxis 2019 ===")
   

    y19_time = responses_y19.select((func.unix_timestamp("tpep_dropoff_datetime") - func.unix_timestamp("tpep_pickup_datetime")), "trip_distance")
    y19_time = y19_time.select(func.col("(unix_timestamp(tpep_dropoff_datetime, yyyy-MM-dd HH:mm:ss) - unix_timestamp(tpep_pickup_datetime, yyyy-MM-dd HH:mm:ss))").cast("int").alias("time"), func.col("trip_distance").cast("int").alias("trip_distance"))
    y19_avg = y19_time.select(func.avg((1000 * func.col("trip_distance")) / func.col("time")))
    
    y19_avg.show()

    y19 = y19_avg.head()[0]

    # ====================================================G20======================================
    print("=== Print Green taxis 2020 ===")


    g20_time = responses_g20.select((func.unix_timestamp("lpep_dropoff_datetime") - func.unix_timestamp("lpep_pickup_datetime")), "trip_distance")
    g20_time = g20_time.select(func.col("(unix_timestamp(lpep_dropoff_datetime, yyyy-MM-dd HH:mm:ss) - unix_timestamp(lpep_pickup_datetime, yyyy-MM-dd HH:mm:ss))").cast("int").alias("time"), func.col("trip_distance").cast("int").alias("trip_distance"))
    g20_avg = g20_time.select(func.avg((1000 * func.col("trip_distance")) / func.col("time")))
    
    g20_avg.show()

    g20 = g20_avg.head()[0]

    # ====================================================Y20======================================
    print("=== Print Yellow taxis 2020 ===")
    

    y20_time = responses_y20.select((func.unix_timestamp("tpep_dropoff_datetime") - func.unix_timestamp("tpep_pickup_datetime")), "trip_distance")
    y20_time = y20_time.select(func.col("(unix_timestamp(tpep_dropoff_datetime, yyyy-MM-dd HH:mm:ss) - unix_timestamp(tpep_pickup_datetime, yyyy-MM-dd HH:mm:ss))").cast("int").alias("time"), func.col("trip_distance").cast("int").alias("trip_distance"))
    y20_avg = y20_time.select(func.avg((1000 * func.col("trip_distance")) / func.col("time")))
    
    y20_avg.show()

    y20 = y20_avg.head()[0]    

    g19_avg = g19_avg.union(y19_avg)
    g19_avg = g19_avg.union(g20_avg)
    g19_avg = g19_avg.union(y20_avg)

    y_axis = {'Green_taxi19': g19, 'Yellow_taxi19' : y19, 'Green_taxi20': g20, 'Yellow_taxi20': y20}
    g19_avg.write.format("csv").save("s3n://aseieddata/avg")
    print(f"\n\n\n{y_axis}\n\n\n")

    

    session.stop()

# {'Green_taxi19': 2.7606352972639128, 'Yellow_taxi19': 2.487639197232751, 'Green_taxi20': 21.230380260467584, 'Yellow_taxi20': 6.337120249430919}
    