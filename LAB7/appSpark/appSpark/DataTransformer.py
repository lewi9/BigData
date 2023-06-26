class DataTransformer:

    @staticmethod
    def cast_to_int(df, col):
        return df.withColumn(col, df[col].cast("int"))

    @staticmethod
    def agg_sum(df, by, col):
        return df.groupby(by).sum(col)

    @staticmethod
    def agg_min(df, by, col):
        return df.groupby(by).min(col)

    @staticmethod
    def agg_max(df, by, col):
        return df.groupby(by).max(col)
