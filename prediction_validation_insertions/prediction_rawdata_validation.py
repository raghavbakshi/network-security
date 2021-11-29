import os
from logger.log_file import logger
import json
import shutil
import pandas as pd


class Validation:
    def __init__(self, path):
        self.prediction_files = path
        self.schema_path = 'schema_prediction.json'
        self.logger = logger()

    def schema_validation(self):
        """
              Method Name: schema_valiation
              Description: This method validates the dataset from the schema file


              Written By: Raghav Bakshi
              Version: 1.0
              Revisions: None

                """
        try:
            with open(self.schema_path, "r") as f:
                dic = json.load(f)
                col_name = dic["col_name"]
                number_of_columns = dic["no_of_columns"]

                file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
                message = "NumberofColumns:: {}".format(number_of_columns)
                self.logger.log(file, message)
                file.close()

        except Exception as e:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        return col_name, number_of_columns

    def name_validation(self):
        """
                   Method Name: name_valiation
                   Description: This method validates the dataset name format.


                   Written By: Raghav Bakshi
                   Version: 1.0
                   Revisions: None

                     """

        lst = ['Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv',
               'Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv', 'Friday-WorkingHours-Morning.pcap_ISCX.csv',
               'Monday-WorkingHours.pcap_ISCX.csv', 'Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv',
               'Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv', 'Tuesday-WorkingHours.pcap_ISCX.csv',
               'Wednesday-WorkingHours.pcap_ISCX.csv', 'prediction.csv']

        try:
            f = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            files = os.listdir(self.prediction_files)
            self.good_data_folder()
            for file in files:
                if file in lst:
                    shutil.copy("prediction_file/" + file, "Prediction_rawfiles_validated/Good_Raw")
                    self.logger.log(f, "Name Validation Complete! File " + "{}".format(file) + "moved to Good Data "
                                                                                               "folder")
                else:
                    shutil.copy("prediction_file/" + file, "Prediction_rawfiles_validated/Bad_Raw")
                    self.logger.log(f,
                                    "Name Validation Complete! File " + "{}".format(file) + "moved to Bad Data folder")
        except Exception as e:
            self.logger.log(f, "Error in validation " + "{}".format(e))

    def good_data_folder(self):

        """
      Method Name: good_data_folder
      Description: This method creates directories to store the Good Data and Bad Data
                    after validating the prediction data.


      Written By: Raghav Bakshi
      Version: 1.0
      Revisions: None

        """
        try:
            path = os.path.join("Prediction_rawfiles_validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.mkdir(path)
            path = os.path.join("Prediction_rawfiles_validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.mkdir(path)
        except Exception as e:
            f = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(f, "Error while creating Directory ")
            f.close()
            raise OSError

    def delete_good_data_folder(self):

        """
      Method Name: delete_good_data_folder
      Description: This method deletes directories which the Good Data.


      Written By: Raghav Bakshi
      Version: 1.0
      Revisions: None

        """

        try:
            f = open("Prediction_Logs/GeneralLog.txt", 'a+')
            path = "Prediction_rawfiles_validated/"
            if os.path.isdir(path + "Good_Raw/"):
                shutil.rmtree(path + "Good_Raw")
                self.logger.log(f, "GoodRaw directory deleted successfully!!!")
                f.close()
        except Exception as e:
            self.logger.log(f, "Error while deleting Good_Raw " + "{}".format(e))

    def col_length(self, numberofcolumns):

        """
              Method Name: col_length
              Description: This method validates column length of the Good_Raw files


              Written By: Raghav Bakshi
              Version: 1.0
              Revisions: None

                """

        try:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            path = "Prediction_rawfiles_validated/"
            for file in os.listdir(path + "Good_Raw/"):
                df = pd.read_csv(path + "Good_Raw/" + file)
                df = df.drop(columns=["Unnamed: 0"])


                if df.shape[1] == numberofcolumns:
                    self.logger.log(f, "Column validation Successful")
                else:
                    shutil.move(path + "Good_Raw/" + file, path + "Bad_Raw/")
                    self.logger.log(f, "Column validation unsuccessful, Files moved to Bad_Raw")
        except Exception as e:
            self.logger.log(f, "Error while column length validation")
