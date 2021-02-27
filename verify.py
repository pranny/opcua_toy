import glob
import logging
import os
import shutil
from os import listdir
from os.path import join, isfile
import pathlib

from pyspark.sql import SparkSession
from pyspark.sql.functions import first


class VerifyDatafetch(object):

    def __init__(self):
        mypath = "./files/"
        self.csv_files = [f for f in listdir(mypath) if isfile(join(mypath, f)) and join(mypath, f).endswith(".csv")]

    def read_merge(self):
        spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .getOrCreate()

        dir = pathlib.Path(__file__).parent.absolute()
        file_path = os.path.join(dir, 'files')
        path = glob.glob("%s/*.csv" % file_path)

        schema = 'node STRING, server_ts STRING, source_ts STRING, val FLOAT'
        df = spark.read.csv(path=path,
                            header=True,
                            sep=",",
                            schema=schema)

        df1 = df.groupby("source_ts").pivot("node").agg(first("val"))
        df2 = df.groupby("server_ts").pivot("node").agg(first("val"))

        if os.path.exists("merged_op.csv"):
            shutil.rmtree("merged_op.csv")
        if os.path.exists("merged_op2.csv"):
            shutil.rmtree("merged_op2.csv")

        df1.coalesce(1).write.csv(path="merged_op.csv", header=True)
        df2.coalesce(1).write.csv(path="merged_op2.csv", header=True)

        shutil.rmtree("files")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    v = VerifyDatafetch()
    v.read_merge()
