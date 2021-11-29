"""
This is the Entry point for Training the Machine Learning Model.

Written By: Raghav Bakshi
Version: 1.0
Revisions: None

"""

# Doing the necessary imports
from sklearn.model_selection import train_test_split
from PCA.pca import scaled_pca
from data_ingestion import data_loader
from data_peprocessing import preprocessing
from logger.log_file import logger
from data_peprocessing import clustering
from best_model_finder import tuner
from file_operations import file_methods

class trainModel:

    def __init__(self):
        self.log_writer = logger()

        self.file_object = open("Training_Logs/ModelTrainingLog.txt", 'a+')

    def trainingModel(self):
        # Logging the start of Training
        self.log_writer.log(self.file_object, 'Start of Training')
        try:
            try:
                # Getting the data from the source
                data_getter = data_loader.Get_data(self.file_object, self.log_writer)
                data = data_getter.get_data()
                self.log_writer.log(self.file_object, 'Data loaded successfully')
            except:
                self.log_writer.log(self.file_object, 'Data loaded failed')
            """doing the data preprocessing"""
            try:
                preprocessor = preprocessing.Preprocessor(self.file_object, self.log_writer)
                data = preprocessor.remove_columns(data, ['serial',
                                                          'Unnamed: 68'])  # remove the unnamed column as it doesn't contribute to prediction.
                self.log_writer.log(self.file_object, 'columns removed successfully')
            except:
                self.log_writer.log(self.file_object, 'columns removal failed')
            # create separate features and labels
            try:
                X, Y = preprocessor.separate_label_feature(data, label_column_name='label')
                self.log_writer.log(self.file_object, 'separation successful ')
            except:
                self.log_writer.log(self.file_object, 'seperation failed')

            try:
                cols_to_drop = preprocessor.get_columns_with_zero_std_deviation(X)
                self.log_writer.log(self.file_object, 'columns with zero std dropped')
            except:
                self.log_writer.log(self.file_object, 'failed in dropping column with zero std')

            try:
            # drop the columns obtained above
                X = preprocessor.remove_columns(X, cols_to_drop)
                self.log_writer.log(self.file_object, 'columns removed')
            except:
                self.log_writer.log(self.file_object, 'column removal failed')

            try:
                print(45)
                sc = scaled_pca(X)
                self.scaled, self.pca, X = sc.scaled_pca()
                print(8962)
                self.log_writer.log(self.file_object, 'pca successful')
            except:
                self.log_writer.log(self.file_object, 'pca failed')

            try:
                file_op = file_methods.File_Operation(self.file_object, self.log_writer)
                save_model = file_op.save_model(self.scaled, "scaler")
                self.log_writer.log(self.file_object, 'model saved scaler')
                save_model = file_op.save_model(self.pca, "pca")
                self.log_writer.log(self.file_object, 'model saved pca')
            except:
                self.log_writer.log(self.file_object, 'model save failed')




            """ Applying the clustering approach"""



            kmeans = clustering.KMeansClustering(self.file_object, self.log_writer)  # object initialization.
            number_of_clusters = kmeans.elbow_plot(X)  # using the elbow plot to find the number of optimum clusters

            # Divide the data into clusters
            X = kmeans.create_clusters(X, number_of_clusters)

            # create a new column in the dataset consisting of the corresponding cluster assignments.
            X['labels'] = Y

            # getting the unique clusters from our dataset
            list_of_clusters = X['Cluster'].unique()
            """parsing all the clusters and looking for the best ML algorithm to fit on individual cluster"""

            for i in list_of_clusters:
                cluster_data = X[X['Cluster'] == i]  # filter the data for one cluster

                # Prepare the feature and Label columns
                cluster_features = cluster_data.drop(['labels', 'Cluster'], axis=1)
                cluster_label = cluster_data['labels']

                # splitting the data into training and test set for each cluster one by one
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=1 / 3,
                                                                    random_state=355)

                model_finder = tuner.Model_Finder(self.file_object, self.log_writer)  # object initialization

                # getting the best model for each of the clusters
                best_model_name, best_model = model_finder.get_best_model(x_train, y_train, x_test, y_test)

                # saving the best model to the directory.
                file_op = file_methods.File_Operation(self.file_object, self.log_writer)
                save_model = file_op.save_model(best_model, best_model_name + str(i))

            # logging the successful Training
            self.log_writer.log(self.file_object, 'Successful End of Training')
            self.file_object.close()

        except Exception:
            # logging the unsuccessful Training
            self.log_writer.log(self.file_object, 'Unsuccessful End of Training')
            self.file_object.close()
            raise Exception

