from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Read from MySQL").config("spark.sql.warehouse.dir", "/user/hive/warehouse").enableHiveSupport().getOrCreate()


jdbcHostname = "savvients-classroom.cefqqlyrxn3k.us-west-2.rds.amazonaws.com"
jdbcPort = 3306
jdbcDatabase = "practical_exercise"
jdbcUsername = "sav_proj"
jdbcPassword = "authenticate"
jdbcUrl = "jdbc:mysql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)


connectionProperties = {
    "user": jdbcUsername, "password": jdbcPassword, "driver": "com.mysql.jdbc.Driver"
}

spark.sql("show databases").show()
spark.sql("use pavan")

df = spark.read.jdbc(url=jdbcUrl, table="user", properties=connectionProperties)
df.write.mode("overwrite").saveAsTable("pavan.userr")
spark.sql("select * from userr limit(5)").show()
df_act =spark.read.jdbc(url=jdbcUrl, table="activitylog", properties=connectionProperties)
df_act.write.mode("overwrite").saveAsTable("pavan.activitylogg")
spark.sql("select * from activitylogg limit(5)").show()
spark.sql("CREATE TABLE IF NOT EXISTS pavan.userdump(user_id bigint, filename string, time_stamp bigint)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored$
#spark.sql("LOAD DATA INPATH '/user/hadoop/pavan/user_upload_dump_2023_03_06.csv' overwrite into table pavan.userdump")
spark.sql("select * from userdump").show()
## USER_REPORT TABLE

spark.sql("CREATE TABLE IF NOT EXISTS pavan.user_report(user_id int, total_updates int, total_inserts int, total_deletes int, last_activity_type string, is_$

spark.sql("""
        INSERT OVERWRITE TABLE user_report
        SELECT
        userr.id AS user_id,
        COALESCE(SUM(CASE WHEN activitylogg.type = 'UPDATE'
	  THEN 1 ELSE 0 END))AS total_updates,
        COALESCE(SUM(CASE WHEN activitylogg.type = 'INSERT'
        THEN 1 ELSE 0 END))AS total_inserts,
        COALESCE(SUM(CASE WHEN activitylogg.type = 'DELETE'
        THEN 1 ELSE 0 END))AS total_deletes,
        MAX(activitylogg.type)AS last_activity_type,
        CASE WHEN CAST(from_unixtime(MAX(activitylogg.timestamp)) AS DATE)>=
        DATE_SUB(CURRENT_TIMESTAMP(),2)THEN true
        ELSE false END AS is_active,
        COALESCE(COUNT(userdump.user_id))AS
        upload_count
        FROM userr
        LEFT jOIN activitylogg ON userr.id=
        activitylogg.user_id
        LEFT JOIN userdump ON userr.id=
        userdump.user_id
        GROUP BY userr.id""")

spark.sql("select * from user_report").show()

##USER_TOTAL TABLE

spark.sql("""CREATE TABLE IF NOT EXISTS pavan.user_total(time_ran timestamp, total_users int,users_added int)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE""")

spark.sql("""
        INSERT INTO user_total
        SELECT
        t1.time_ran,
        t1.total_users,
        t1.total_users-COALESCE((
        t2.total_users,0) AS users_added
        FROM(
        SELECT CURRENT_TIMESTAMP()AS time_ran,
        COUNT(*)AS total_users
        FROM userr
        )t1
        LEFT JOIN (
        SELECT time_ran, total_users
        FROM user_total
        )t2 ON t1.time_ran > t2.time_ran
        ORDER BY t1.time_ran
        """)
spark.sql("select * from user_total").show()

