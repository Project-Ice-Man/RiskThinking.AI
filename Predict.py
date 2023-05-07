from flask import Flask, request

import pandas as pd, time
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

app = Flask(__name__)

@app.route('/predict')
def predict():
    try:
        start_time = time.time()

        # query string | parameters
        args = request.args

        # query string | values
        Symbol                = args.get('Symbol')
        vol_moving_avg        = args.get('vol_moving_avg')
        adj_close_rolling_med = args.get('adj_close_rolling_med')

        # train files saved during pipeline execution in folder /Problem 3/
        X_train = pd.read_pickle(f'/Volumes/Windows/Win/Code/RiskThinking.AI/Problem 3/{Symbol}/X_train.pkl')
        y_train = pd.read_pickle(f'/Volumes/Windows/Win/Code/RiskThinking.AI/Problem 3/{Symbol}/y_train.pkl')

        # Create a RandomForestRegressor model
        model = RandomForestRegressor(n_estimators=100, random_state=42)

        # Train the model
        model.fit(X_train, y_train)

        # test files saved during pipeline execution in folder /Problem 3/
        X_test = pd.read_pickle(f'/Volumes/Windows/Win/Code/RiskThinking.AI/Problem 3/{Symbol}/X_test.pkl')
        y_test = pd.read_pickle(f'/Volumes/Windows/Win/Code/RiskThinking.AI/Problem 3/{Symbol}/y_test.pkl')

        # Make predictions on test data
        y_pred = model.predict(X_test)

        # Calculate the Mean Absolute Error and Mean Squared Error
        mae = mean_absolute_error(y_test, y_pred)
        mse = mean_squared_error(y_test, y_pred)

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
                            f'<td class=right>   Mean Absolute Error  <td> {mae:,.2f}',
                            f'<td class=right>   Mean  Squared Error  <td> {mse:,.2f}',
                            f'<td class=right>        Execution Time  <td> {round(time.time() - start_time, 2):.2f} sec' ]) +

            '\n</table>\n<style> td, th {font: 9pt Verdana} th {text-align: left; background-color: lemonchiffon} .right {text-align: right; padding-right: 10} </style>')

    # in case something bad happened
    except Exception as x:
        return f'<pre>{x}</pre>'

if __name__ == "__main__":
    app.run()
