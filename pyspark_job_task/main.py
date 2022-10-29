

from pyspark.sql import SparkSession, DataFrame
from functools import reduce

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    df1 = spark.read.csv("seclog/day_01.csv")
    df2 = spark.read.csv("seclog/day_02.csv")
    df3 = spark.read.csv("seclog/day_03.csv")
    dfs = [df1, df2, df3]

    df_complete = reduce(DataFrame.unionAll, dfs)

    col_names = ["ip", "date", "time", "zone", "cik", "accession", "doc", "code", "filesize", "idx",
                 "norefer", "noagent", "find", "crawler", "browser"]

    for i, name in enumerate(col_names):
        df_complete = df_complete.withColumnRenamed(f"_c{i}", name)

    df_complete.show()