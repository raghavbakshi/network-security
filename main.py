from training_validation_insertion import Pred_validation
from train_model import trainModel
from prediction_validation_insertion import Pred_validations
from prediction_fromModel import prediction

pred_val = Pred_validation(r"E:\full stack\internship\network security\dataset")
pred_val.prediction_validation()

train = trainModel()
train.trainingModel()

preds_val = Pred_validations(r"E:\full stack\internship\network security\prediction_file")
preds_val.prediction_validation()

pred = prediction(r"E:\full stack\internship\network security\Prediction_FileFromDB\InputFile.csv")
pred.predictionFromModel()



