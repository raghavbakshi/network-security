from prediction_validation_insertions.prediction_rawdata_validation import Validation
#import prediction_validation_insertions
from Data_transformation_prediction.Data_transformation import Transformation
from Data_validation_insertion_prediction.validation_insertion_prediction import DataBaseOperation
from sampling.sample_training import Sampling
from logger.log_file import logger


class Pred_validations:

    def __init__(self, path):
        self.raw_data = Validation(path)
        self.transformation = Transformation()
        self.db_operation = DataBaseOperation()
        self.sampling = Sampling()
        self.file_object = open("Prediction_Logs/Training_Log.txt", 'a+')
        self.log_writer = logger()

    def prediction_validation(self):

        self.log_writer.log(self.file_object, "Start of Validation for Training")

        # validating file name
        try:
            self.raw_data.name_validation()
            self.log_writer.log(self.file_object, "Name validation complete")
        except:
            self.log_writer.log(self.file_object, "Error in name validation")

        # Extracting column names
        try:
            col_name, number_of_column = self.raw_data.schema_validation()
            self.log_writer.log(self.file_object, "Column information extracted")
        except:
            self.log_writer.log(self.file_object, "Failed in extracting column information")

        # Dataset column validation
        try:
            self.raw_data.col_length(number_of_column)
            self.log_writer.log(self.file_object, "Column length validated")
        except:
            self.log_writer.log(self.file_object, "Column length validation failed")

        # Missing values imputation
        try:
            self.transformation.missing_value_and_serial()
            self.log_writer.log(self.file_object, "missing values handled")
        except:
            self.log_writer.log(self.file_object, "failed in handling the missing values")

        # Establish connection
        try:
            self.db_operation.database_connection()
            self.log_writer.log(self.file_object, "Connection established successfully")
        except:
            self.log_writer.log(self.file_object, "failed in establishing connection")

        # create table
        try:
            self.db_operation.create_table()
            self.log_writer.log(self.file_object, "Table created successfully")
        except:
            self.log_writer.log(self.file_object, "table creation failed")


        # Missing values imputation
        try:
            self.transformation.missing_value_and_serial()
            self.log_writer.log(self.file_object, "missing values handled")
        except:
            self.log_writer.log(self.file_object, "failed in handling the missing values")

        # insertion in table
        try:
            self.db_operation.insertIntoTableGoodData()
            self.log_writer.log(self.file_object, "values inserted succesfully")
        except:
            self.log_writer.log(self.file_object, "failed in inserting values to the table")

        # loading data to csv
        try:
            self.db_operation.selectingDatafromtableintocsv()
            self.log_writer.log(self.file_object, "Data loaded successfully")
        except:
            self.log_writer.log(self.file_object, "failed in loading data")





