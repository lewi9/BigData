class DataWriter:


    @staticmethod
    def write_csv(df, file_path, header=True, mode="overwrite"):
        df.write.csv(file_path, header=header, mode=mode)
