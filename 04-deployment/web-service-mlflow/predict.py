import os
import pickle

import mlflow
from mlflow.tracking import MlflowClient
from flask import Flask, request, jsonify

mlflow.set_tracking_uri("http://127.0.0.1:5000")
mlflow.set_experiment("green-taxi-duration")

MLFLOW_TRACKING_URI = 'http://127.0.0.1:5000'

#client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)

#client.download_artifacts(run_id="8cc710867a3a43849fdb317e1adefb45")

logged_model = 'runs:/8cc710867a3a43849fdb317e1adefb45/model'

# Load model as a PyFuncModel
model = mlflow.pyfunc.load_model(logged_model)


def prepare_features(ride):
    features = {}
    features['PU_DO'] = '%s_%s' % (ride['PULocationID'], ride['DOLocationID'])
    features['trip_distance'] = ride['trip_distance']
    return features


def predict(features):
    preds = model.predict(features)
    return float(preds[0])


app = Flask('duration-prediction')


@app.route('/predict', methods=['POST'])
def predict_endpoint():
    ride = request.get_json()

    features = prepare_features(ride)
    pred = predict(features)

    result = {
        'duration': pred,
        'model_version': "8cc710867a3a43849fdb317e1adefb45"
    }

    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=9696)
