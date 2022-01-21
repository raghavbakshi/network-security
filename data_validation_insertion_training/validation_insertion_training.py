from logger.log_file import logger
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os
import pandas as pd
import csv
import shutil

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

cloud_config = {
    'secure_connect_bundle': 'secure-connect-network-security.zip'
}
auth_provider = PlainTextAuthProvider('nkRFjxeEoekFGeBtHvMDpuZu',
                                      '_Cg.SwbGgdiC3KAj.UGHnZFkpG,cuLDjdMO_ybj,o97fyKv6kMCwZSty_IlgcPwStZ_i4XJW,CQLE4s67eX9pYXq.Hz0PswLLFnFBeUD_mHt5jfLJ-+bKRMQPBb9AWjD')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)


class DataBaseOperation:
    """
        This class is responsible to create all DB operations
        """

    def __init__(self):
        self.path = 'Training_Database/'
        self.badFilePath = "Training_rawfiles_validated/Bad_Raw"
        self.goodFilePath = "Training_rawfiles_validated/Good_Raw"
        self.logger = logger()

    def database_connection(self):
        """
            Method Name: databaseConnection
            Description: This method opens the database with the given name
            Output: Connection to the DB
            On Failure: Raise ConnectionError

            Written By: Raghav Bakshi
            Version: 1.0
            Revisions: None
            """

        try:

            session = cluster.connect()
            file = open('Training_Logs/DataBaseConnectionLog.txt', 'a+')
            self.logger.log(file, 'Connection established successfully')
            file.close()
        except ConnectionError:
            file = open('Training_Logs/DataBaseConnectionLog.txt', 'a+')
            self.logger.log(file, 'Error while establishing connection')
            file.close()
            raise ConnectionError

    def create_table(self):
        """


        Method Name: createTableDb
        Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
        Output: None
        On Failure: Raise Exception

        Written By: Raghav Bakshi
        Version: 1.0
        Revisions: None
        """
        file = open('Training_Logs/DbTableCreateLog.txt', 'a+')
        try:
            session = cluster.connect()
            for row in session.execute("SELECT keyspace_name, table_name FROM   system_schema.tables;"):
                if row[0] == "data":
                    if row[1] == "data":

                        self.logger.log(file, 'table created successfully 1 ')
                        file.close()
                        break
                    else:
                        path = "Training_rawfiles_validated/Good_Raw/"
                        for files in os.listdir(path):
                            df = pd.read_csv(path + files)
                            df.drop(columns=['Unnamed: 0'], inplace=True)

                            break
                        empty_columns = []
                        for i in df.columns:
                            if df[i].isin([0]).sum(axis=0) == 1500:
                                empty_columns.append(i)
                        df = df.drop(columns=empty_columns)
                        lst = [i for i in range(1, df.shape[0] + 1)]
                        df.insert(0, "serial", lst, True)

                        query = ""
                        for column in df.columns:
                            if column[0] == ' ':
                                column = column[1:]
                            column = column.replace(' ', '_')
                            column = column.replace('/', '_')
                            column = column.replace('.', '_')

                            if column == "serial":
                                query += column + " decimal PRIMARY KEY,"
                            elif column == "Label":
                                query += column + " varchar"
                            else:
                                query += column + " decimal,"
                        session.execute(
                            'CREATE TABLE data.data(' + query + ');').one()
                        file = open('Training_Logs/DbTableCreateLog.txt', 'a+')
                        self.logger.log(file, 'table created successfully1')
                        file.close()
                        break
        except Exception as e:
            file = open('Training_Logs/DbTableCreateLog.txt', 'a+')
            self.logger.log(file, 'Error while creating table')
            file.close()
            raise e

    def insertIntoTableGoodData(self):

        """
                               Method Name: insertIntoTableGoodData
                               Description: This method inserts the Good data files from the Good_Raw folder into the
                                            above created table.
                               Output: None
                               On Failure: Raise Exception

                                Written By: Raghav Bakshi
                               Version: 1.0
                               Revisions: None

        """
        log_file = open("Training_Logs/DbInsertLog.txt", 'a+')
        try:
            session = cluster.connect()
            count_file_row = 0
            empty_columns = []
            path = "Training_rawfiles_validated/Good_Raw/"
            df = pd.read_csv(path + 'final data')
            for i in df.columns:
                if df[i].isin([0]).sum(axis=0) == 1500:
                    empty_columns.append(i)
            df.drop(columns=empty_columns, inplace=True)
            df.drop(columns=["Unnamed: 0"], inplace=True)
            lst = [i for i in range(1, df.shape[0] + 1)]
            df.insert(0, "serial", lst, True)
            for i in df.columns:
                df[i].fillna(df[i].mode()[0], inplace=True)
            try:
                for i in range(df.shape[0]):
                    count_file_row += 1
                    query = 'insert into data.data('
                    for column in df.columns:

                        if column[0] == ' ':
                            column = column[1:]

                        column = column.replace(' ', '_')
                        column = column.replace('/', '_')
                        column = column.replace('.', '_')
                        query = query + column + ","

                    query = query[:-1]
                    query = query + ') values({});'

                    variable = str((df.iloc[i]).values).replace(' ', ',')
                    variable = variable.replace('[', '')
                    variable = variable.replace(']', '')
                    query = query.format(variable)
                    query = query.replace("inf", "0")
                    session.execute(query)



            except Exception as e:
                self.logger.log(log_file, "Error while inserting in table " + e)
                raise e


        except Exception as e:
            self.logger.log(log_file, "Error while inserting in table" + e)
            log_file.close()
            raise e
        log_file.close()

    def selectingDatafromtableintocsv(self):

        """
                               Method Name: selectingDatafromtableintocsv
                               Description: This method exports the data in GoodData table as a CSV file. in a given location.
                                            above created .
                               Output: None
                               On Failure: Raise Exception

                                Written By: iNeuron Intelligence
                               Version: 1.0
                               Revisions: None

        """

        self.fileFromDb = 'Training_FileFromDB/'
        self.fileName = 'InputFile.csv'
        log_file = open("Training_Logs/ExportToCsv.txt", 'a+')
        session = cluster.connect()
        try:

            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            with open(self.fileFromDb + self.fileName, 'w', encoding='utf-8') as csvFile:
                for column_name in session.execute("SELECT * FROM data.data;").column_names:
                    csvFile.writelines(column_name + ',')
                csvFile.writelines('\n')
                for i in session.execute("SELECT * FROM data.data;").all():
                    for j in i:
                        csvFile.writelines(str(j) + ',')
                    csvFile.writelines('\n')

            self.logger.log(log_file, "File exported successfully!!!")
            log_file.close()

        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error : %s" % e)
            log_file.close()
