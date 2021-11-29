from logger.log_file import logger
import pandas as pd
import os
import shutil

class Sampling:

    def sample(self):
        """
                    Method Name: sample
                    Description: This method is used to take a sample from the dataset
                    On Failure: Raise ConnectionError

                    Written By: Raghav Bakshi
                    Version: 1.0
                    Revisions: None
                    """
        f = open("Training_Logs/GeneralLog.txt", 'a+')
        try:
            directory = os.getcwd()

            path = "/Training_rawfiles_validated/Good_Raw/"

            df = pd.DataFrame()
            os.chdir(directory + path)

            for i in os.listdir():
                df = pd.concat([df, pd.read_csv(i)])

            lst = df[" Label"].unique()
            df1 = pd.DataFrame()

            for i in lst:
                df1 = pd.concat([df1, df[df[" Label"] == i].sample(15, replace=True)])


            os.chdir(directory)
            if 'Training_rawfiles_validated' not in os.listdir():
                os.mkdir('Training_rawfiles_validated')

            os.chdir('Training_rawfiles_validated')
            if 'Good_Raw' not in os.listdir():
                os.mkdir('Good_Raw')
            else:
                shutil.rmtree('Good_Raw')
                os.makedirs('Good_Raw')

            os.chdir('Good_Raw')
            # empty_columns = []
            # for i in df1.columns:
            #     if df1[i].isin([0]).sum(axis=0) == 1500:
            #         empty_columns.append(i)
            # df1 = df1.drop(columns=empty_columns)
            df1.to_csv("final data", header=True, index=False)
            os.chdir(directory)


        except Exception as e:
            logger.log(f, "sampling failed due to {}".format(e))





