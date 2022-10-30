from typing import Optional, Tuple, List
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from matplotlib import pyplot as plt


def get_users_with_longest_sessions(in_df: DataFrame, n_top_users: int = 10) -> DataFrame:

    in_df = in_df.drop("cik", "accession", "doc", "code", "filesize", "idx", "norefer", "noagent", "find",
                 "crawler", "browser")

    user_parti = Window.partitionBy("ip").orderBy("ts")

    # дописываем предыдущее значение
    df_with_lags = in_df.withColumn("lag", lag("ts").over(user_parti))

    # Фильтруемся по разницам времени с предыдущим действием, оставляем только те действия,
    # которые начались через 30+ минут после предыдущего: это и есть начало сессий
    # (как и действия, у которых нет предыдущих)
    session_starts = df_with_lags.filter(col("lag").isNotNull()).withColumn(
        "ts_lag", df_with_lags.ts - df_with_lags.lag).filter(col("ts_lag") > expr("INTERVAL 30 MINUTES")).drop("ts_lag")
    session_starts = session_starts.union(df_with_lags.filter(df_with_lags.lag.isNull()))

    # Творим подобную же фигню для окончаний сессий
    df_with_leads = in_df.withColumn("lead", lead("ts").over(user_parti))
    session_ends = df_with_leads.filter(col("lead").isNotNull()).withColumn(
        "ts_lead", df_with_leads.lead - df_with_leads.ts).filter(col("ts_lead") > expr("INTERVAL 30 MINUTES")).drop("ts_lead")
    session_ends = session_ends.union(df_with_leads.filter(df_with_leads.lead.isNull()))

    # Собираем табличку с началами и концам сессий, добавляем галочку, что это именно окончание сессии...
    session_limits = session_starts.drop("lag").withColumn("is_end", expr('false')).union(
        session_ends.drop("lead").withColumn("is_end", expr('true')))

    user_session_parti = Window.partitionBy("ip").orderBy("ts", "is_end")

    session_lengths = session_limits.withColumn("session_length", lead("ts").over(user_session_parti) - col("ts")).filter(~col("is_end"))
    session_lengths = session_lengths.withColumn("long_session_length", col("session_length").cast("long"))

    users_with_longest_sessions = session_lengths.groupBy("ip")\
        .agg(max(col("long_session_length")).alias("max_session_length"))\
        .orderBy(desc("max_session_length")).limit(n_top_users)

    return users_with_longest_sessions  # см. колонку max_session_length


def get_users_with_top_downloads(raw_df: DataFrame, n_top_users: int = 10) -> DataFrame:
    df = raw_df.filter(raw_df.code == 200)
    top_downloaders = df.groupBy("ip").agg(sum("filesize").alias("total_downloaded"))
    top_downloaders = top_downloaders.select("ip", "total_downloaded").sort(desc("total_downloaded"))
    return top_downloaders


def get_people_by_hours_distribution(df: DataFrame) -> Tuple[List[int], List[int]]:
    df = df.withColumn("time_only", date_format(df.ts.cast("STRING"), 'HH:mm:ss'))\
        .withColumn("date_only", date_format(df.ts.cast("STRING"), 'yyyy-MM-dd'))

    df = df.select("ip", "ts", "date_only", "time_only")

    n_users_by_hour = []
    max_daily_users_by_hour = []

    for hour in range(24):
        hour_df = df.filter((df.time_only >= f"{hour:02d}:00:00") & (df.time_only < f"{hour + 1:02d}:00:00"))
        n_users_by_hour.append(hour_df.select("ip").distinct().count())

        temp = hour_df.groupBy("date_only").agg(countDistinct("ip").alias("ips_on_date_and_hour")).select(
            max("ips_on_date_and_hour").alias("max_users_on_hour"))
        max_daily_users_by_hour.append(temp.toPandas()["max_users_on_hour"][0])

    return n_users_by_hour, max_daily_users_by_hour


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    # Берём только первые три дня, чтобы не дохнуть при локальной отладке = )
    df1 = spark.read.csv("seclog/day_01.csv")
    df2 = spark.read.csv("seclog/day_02.csv")
    df3 = spark.read.csv("seclog/day_03.csv")
    dfs = [df1, df2, df3]
    df_complete = reduce(DataFrame.unionAll, dfs)

    # Слегка почистим данные:
    col_names = ["ip", "date", "time", "zone", "cik", "accession", "doc", "code", "filesize", "idx",
                 "norefer", "noagent", "find", "crawler", "browser"]

    df = df_complete
    for i, name in enumerate(col_names):
        df = df.withColumnRenamed(f"_c{i}", name)

    # Конвертируем строку даты + строку времени в одну колонку формата timestamp
    df = df.withColumn("ts", to_timestamp(format_string("%s %s", df.date, df.time)))
    df = df.drop("date", "time", "zone")

    # Конвертируем ещё кое-что из DOUBLE в инты, потому как схему для CSV делать было лениво
    df = df.withColumn("code", df.code.cast("integer"))
    df = df.withColumn("filesize", df.code.cast("long"))

    # Убираем юзеров-поисковых-ботов
    df = df.filter(df.crawler == 0)

    # Опционально: берём только первые 100 юзеров, чтобы быстрее отлаживаться
    n_users_total = 100

    some_ips = df.select("ip").distinct()
    if n_users_total is not None:
        some_ips = some_ips.limit(n_users_total)

        # df.select("ip").distinct().limit(100).toPandas().to_parquet("some_ips.parquet")
        # some_ips = spark.read.parquet("some_ips.parquet")
        df.createOrReplaceTempView("df")
        some_ips.createOrReplaceTempView("some_ips")
        df_some_ips = spark.sql("SELECT * FROM df WHERE ip IN (SELECT * FROM some_ips)")
    else:
        df_some_ips = df
    df_some_ips.cache()

    # print("LONGEST SESSIONS:")
    # users_with_longest_sessions = get_users_with_longest_sessions(df_some_ips)
    # users_with_longest_sessions.show()

    # print("TOP DOWNLOADS:")
    # users_wth_top_downloads = get_users_with_top_downloads(df_some_ips)
    # users_wth_top_downloads.show()


    print("PEOPLE BY HOURS DISTRIBUTION")
    total_people_by_hours, max_daily_people_by_hours = get_people_by_hours_distribution(df_some_ips)
    print("total_people_by_hours:")
    print(total_people_by_hours)
    print("max_daily_people_by_hours")
    print(max_daily_people_by_hours)


