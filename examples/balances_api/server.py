from flask_api import FlaskAPI
from flask import request
from actions import balances, token_balances
from config import TRANSACTIONS_TABLE, TOKEN_TRANSACTIONS_TABLE

app = FlaskAPI(__name__)

@app.route('/balances/', methods=['POST'])
def get_balances():
    addresses = request.get_json(force=True)
    query = balances.Balances(TRANSACTIONS_TABLE)
    return query.get_balances(addresses)

@app.route('/token_balances/<token>', methods=['POST'])
def get_token_balances(token):
    addresses = request.get_json(force=True)
    query = token_balances.TokenBalances(TOKEN_TRANSACTIONS_TABLE)
    return query.get_balances(addresses, token)

if __name__ == "__main__":
    app.run(debug=True)