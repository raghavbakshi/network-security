from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import pandas as pd
from logger.log_file import logger
from file_operations import file_methods

f = open("Training_Logs/pca.txt", 'a+')
class scaled_pca:
    def __init__(self, data):

        self.log_writer = logger()
        self.file_object = open("Training_Logs/pca.txt", 'a+')
        self.data = data
        self.logger = logger()
        self.scaler = StandardScaler()
        self.pca = PCA(n_components=20)
    def scaled_pca(self):
        try:

            # self.data = self.data.drop(columns=["label", 'Unnamed: 68'])
            # print(1)
            # self.logger.log(f, "column drop successful")
            self.data = self.scaler.fit_transform(self.data)
            self.logger.log(f, "data scale successful")
            self.data = self.pca.fit_transform(self.data)
            self.logger.log(f, "data pca successful")
            self.data = pd.DataFrame(self.data)
            self.logger.log(f, "dataframe successful")
            file_op = file_methods.File_Operation(self.file_object, self.log_writer)
            save_model = file_op.save_model(self.scaler, "scaler")
            file_op = file_methods.File_Operation(self.file_object, self.log_writer)
            save_model = file_op.save_model(self.pca,"pca")
            return self.scaler, self.pca, self.data
        except Exception as e:
            print(e)

            self.logger.log(f, "process failed")