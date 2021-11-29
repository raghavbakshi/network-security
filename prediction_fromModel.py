import pandas
from file_operations import file_methods
from data_peprocessing import preprocessing
from data_ingestion import data_loader
from logger.log_file import logger
from prediction_validation_insertions.prediction_rawdata_validation import Validation
import pandas as pd

class prediction:

    def __init__(self, path):
        self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log_writer = logger()
        if path is not None:
            self.pred_data_val = Validation(path)

    def predictionFromModel(self):

        try:
            self.log_writer.log(self.file_object, 'Start of Prediction')
            data_getter = data_loader.Get_data(self.file_object, self.log_writer)
            data = data_getter.get_data()

            preprocessor = preprocessing.Preprocessor(self.file_object, self.log_writer)
            data = preprocessor.remove_columns(data, ['Unnamed: 67'])
            serial_names = list(data['serial'])


            # cols_to_drop = preprocessor.get_columns_with_zero_std_deviation(data)
            # data = preprocessor.remove_columns(data, cols_to_drop)


            file_loader = file_methods.File_Operation(self.file_object, self.log_writer)
            pca = file_loader.load_model('pca')
            scaler = file_loader.load_model('scaler')
            kmeans = file_loader.load_model('KMeans')

            data = scaler.fit_transform(data)
            data = pca.fit_transform(data)
            data = pd.DataFrame(data)
            clusters = kmeans.predict(data)  # drops the first column for cluster prediction
            data['clusters'] = clusters
            clusters = data['clusters'].unique()
            for i in clusters:
                cluster_data = data[data['clusters'] == i]
                # cluster_data = data.drop(labels=['serial'], axis=1)
                cluster_data = cluster_data.drop(['clusters'], axis=1)
                model_name = file_loader.find_correct_model_file(i)
                model = file_loader.load_model(model_name)
                result = list(model.predict(cluster_data))
                result = pandas.DataFrame(list(zip(serial_names, result)), columns=['serial', 'Prediction'])
                path = "Prediction_Output_File/Predictions.csv"
                result.to_csv("Prediction_Output_File/Predictions.csv", header=True, mode='a+')  # appends result to
                # prediction file
            self.log_writer.log(self.file_object, 'End of Prediction')
        except Exception as ex:
            self.log_writer.log(self.file_object, 'Error occurred while running the prediction!! Error:: %s' % ex)
            raise ex
        return path, result.head().to_json(orient="records")
