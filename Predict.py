from flask import Flask, request

import pandas as pd, time, pickle, json
from sklearn.ensemble import RandomForestRegressor

app = Flask(__name__)

@app.route('/predict')
def predict():
    try:
        start_time = time.time()

        # read
        read_path = '/Volumes/Windows/Win/Code/RiskThinking.AI'

        # query string | parameters
        args = request.args

        # query string | values
        Symbol                = args.get('Symbol')
        vol_moving_avg        = args.get('vol_moving_avg')
        adj_close_rolling_med = args.get('adj_close_rolling_med')

        # blanks
        error, model = {}, RandomForestRegressor(n_estimators=100, random_state=42)

        # loading the trained model
        with open(f'{read_path}/Problem 3/{Symbol}/model.pkl', 'rb') as f:
            model = pickle.load(f)

        # loading the error info
        with open(f'{read_path}/Problem 3/{Symbol}/error.json', 'r') as f:
            error = json.load(f)

        # predicting
        pred = ( model.predict(
            pd.DataFrame({'vol_moving_avg': [vol_moving_avg],
                   'adj_close_rolling_med': [adj_close_rolling_med]}) ))
        # html
        return ('<table>\n<tr>' +

            '\n<tr>'.join([ f'<td class=right>                Symbol  <td> {Symbol}',
                            f'<td class=right>        vol_moving_avg  <td> {float(vol_moving_avg):,.2f}',
                            f'<td class=right> adj_close_rolling_med  <td> {float(adj_close_rolling_med):,.6f}',
                            f'<td class=right>      Predicted Volume  <th> {int(pred[0]):,}',
                            f'<td class=right>   Mean Absolute Error  <td> {float(error["mean_absolute_error"]):,.2f}',
                            f'<td class=right>   Mean  Squared Error  <td> {float(error["mean_squared_error"]):,.2f}',
                            f'<td class=right>        Execution Time  <td> {round(time.time() - start_time, 2):.2f} sec' ]) +

            '\n</table>\n<style> td, th {font: 9pt Verdana} th {text-align: left; background-color: lemonchiffon} .right {text-align: right; padding-right: 10} </style>'
        )

    # in case something bad happened
    except Exception as x:
        return f'<pre>{x}</pre>'

if __name__ == "__main__":
    app.run()
