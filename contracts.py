import requests
import os
from subprocess import call
import pdb
from time import sleep

SERVER_URL = "http://localhost:3000/{}"
ADD_ABI_URL = SERVER_URL.format("add_abi/{}")
DECODE_PARAMS_URL = SERVER_URL.format("decode_params/{}")

class Contracts():
  def __init__(self):
    self._restart_server()

  def _restart_server(self):
    os.system("kill `lsof -i tcp:3000 | awk 'NR == 2 {print $2}'`")
    call('node ethereum_contracts_server/server.js &', shell=True)
    sleep(3)

  def _add_contract_abi(self, address):
    response = requests.get(ADD_ABI_URL.format(address))
    return response.json()

  def _decode_input(self, encoded_params):
    return requests.get(DECODE_PARAMS_URL.format(encoded_params)).json()