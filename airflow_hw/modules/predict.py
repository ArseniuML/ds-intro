import logging
import os
import sys
import json
import dill
import pandas as pd

path = os.environ.get('PROJECT_PATH', '.')


def predict(timestamp):
    with open(f'{path}/data/models/cars_pipe_{timestamp}.pkl', 'rb') as file:
        model = dill.load(file)

    ids = []
    prices = []
    price_cats = []

    test_dir = f'{path}/data/test'
    for filename in os.listdir(test_dir):
        with open(os.path.join(test_dir, filename)) as json_file:
            X_dict = json.load(json_file)
            X = pd.DataFrame([X_dict])
            y = model.predict(X)
            ids.append(X_dict['id'])
            prices.append(X_dict['price'])
            price_cats.append(y[0])

    df = pd.DataFrame({'id': ids, 'price': prices, 'price_category': price_cats})
    prediction_filename = f'{path}/data/predictions/cars_{timestamp}.csv'
    df.to_csv(prediction_filename, index_label=False, index=False)

    logging.info(f'Prediction is saved as {prediction_filename}')


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        predict(sys.argv[1])
    else:
        print('Provide timestamp as first agrgument')
