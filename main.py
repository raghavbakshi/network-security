from flask import Flask, request, render_template
from flask import Response
#from flask_cors import cross_origin
from training_validation_insertion import Pred_validation
from train_model import trainModel
from prediction_validation_insertion import Pred_validations
from prediction_fromModel import prediction

app = Flask(__name__)


@app.route("/", methods=['GET'])
#@cross_origin()
def home():
    return render_template('login.html')


@app.route("/", methods=['POST'])
#@cross_origin()
def predictRouteClient():
    if request.method == 'POST':
        try:
            path = request.form['Prediction Path']
            preds_val = Pred_validations(r"E:\full stack\internship\network security\prediction_file")
            preds_val.prediction_validation()
            # path = "Prediction_FileFromDB\InputFile.csv"
            pred = prediction(path)
            pred.predictionFromModel()

        except ValueError:
            return Response("Error Occurred! %s" % ValueError)
        except KeyError:
            return Response("Error Occurred! %s" % KeyError)
        except Exception as e:
            return Response("Error Occurred! %s" % e)

    return render_template('results.html')
@app.route("/train", methods=['POST'])
#@cross_origin()
def trainRouteClient():
    try:


        pred_val = Pred_validation(r"E:\full stack\internship\network security\dataset")  # object initialization
        pred_val.prediction_validation()  # calling the training_validation function

        train = trainModel()  # object initialization
        train.trainingModel()  # training the model for the files in the table



    except ValueError:

        return Response("Error Occurred! %s" % ValueError)



if __name__ == "__main__":
    app.run(debug=True)