class DataReader:


    def __init__(self, spark_session):
        self.spark = spark_session

    def read_csv(self, file_path, header=True):
        return self.spark.read.option("header", header).csv(file_path)

    #...