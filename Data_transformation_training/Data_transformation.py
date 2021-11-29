from logger.log_file import logger
import os
import pandas as pd

class Transformation:

    def __init__(self):
        self.goodDataPath = "Training_rawfiles_validated/Good_Raw"
        self.logger = logger()

    def missing_value_and_serial(self):
        """
                      Method Name: missing_value_and_serial
                      Description: This method fills the coumns with missing values and add serial for database.


                      Written By: Raghav Bakshi
                      Version: 1.0
                      Revisions: None

                        """

        f = open("Training_Logs/Missing_values_logs.txt", 'a+')
        self.logger.log(f, "Missing values !")
        try:

            self.logger.log(f, "Missing values imputation started!!")
            path = "Training_rawfiles_validated/Good_Raw"
            for files in os.listdir(path):
                df = pd.read_csv(self.goodDataPath + "/" + files)
                try:
                    for column in df.columns:
                        if column == " Label":
                            df[" Label"].fillna(df[" Label"].mode()[0], inplace=True)
                        else:
                            df[column].fillna(df[column].mean(), inplace=True)
                except:
                    pass
                # length = df.shape[0]
                # lst = [i for i in range(1, length + 1)]
                # df["serial"] = lst
                # df.insert(0, "serial", lst, True)

                self.logger.log(f, "Missing values Imputed for file : {}".format(files))


        except Exception as e:
            self.logger.log(f, "Missing value imputation failed!!")




