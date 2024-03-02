#!/usr/bin/env python
# @desc : 
__coding__ = "utf-8"
__author__ = "bytedance"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# 1.创建 Spark session
sc = SparkSession.builder.appName("SparkProject").getOrCreate()

# 2.定义数据内容
data = [
    ('ABC17969(AB)', '1', 'ABC17969', 2022),
    ('ABC17969(AB)', '2', 'CDC52533', 2022),
    ('ABC17969(AB)', '3', 'DEC59161', 2023),
    ('ABC17969(AB)', '4', 'F43874', 2022),
    ('ABC17969(AB)', '5', 'MY06154', 2021),
    ('ABC17969(AB)', '6', 'MY4387', 2022),
    ('AE686(AE)', '7', 'AE686', 2023),
    ('AE686(AE)', '8', 'BH2740', 2021),
    ('AE686(AE)', '9', 'EG999', 2021),
    ('AE686(AE)', '10', 'AE0908', 2021),
    ('AE686(AE)', '11', 'QA402', 2022),
    ('AE686(AE)', '12', 'OM691', 2022)
]

# 3.创建 DataFrame
df = sc.createDataFrame(data, ["peer_id", "id_1", "id_2", "year"])

# 4.定义函数实现
def calculate_year_count(df, size):
    # 步骤 1：对于每个 peer_id，获取包含 id_2 的年份
    filtered_df = df.filter(col("peer_id").contains(col("id_2")))

    # 步骤 2：给定大小数，计算每个 peer_id 每个年份的数量
    result_df = filtered_df.groupBy("peer_id", "year").count() \
        .withColumn("count", when(col("year") <= filtered_df.select("year").distinct().first()[0], col("count")).otherwise(0))

    # 步骤 3：按年对步骤 2 中的值进行排序，并检查第一年的计数是否大于或等于给定的尺寸数
    result_df = result_df.orderBy("year", ascending=False)
    year_list = []
    year_sum = 0
    for row in result_df.collect():
        year_sum += row["count"]
        if year_sum >= size:
            year_list.append((row["peer_id"], row["year"]))
            break
        else:
            year_list.append((row["peer_id"], row["year"]))
    return year_list

if __name__ == '__main__':

    size_1 = 3
    result_1 = calculate_year_count(df, size_1)
    print(result_1)

    # 5.释放资源
    sc.stop()

