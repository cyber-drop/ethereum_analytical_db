var decoder = new require('abi-decoder')
var express = require('express');
var app = express();
var read = require('read-file');
var cmd = require('node-run-cmd');
var fs = require('fs');

var ABI_CACHE = "/home/anatoli/.quickBlocks/cache/abis/"
var GRAB_ABI_PATH = "../quickBlocks/bin/grabABI"

function getABIFile(address) {
  return ABI_CACHE + address + ".json"
}

app.post("/add_abi", function(request, response) {
  console.log(request)
  var abiJSON = JSON.parse(request.body)
  decoder.addABI(abiJSON)
  response.json({
    "added": true
  })
});

app.get("/get_abi/:address", function(request, response) {
  var address = request.params.address
  cmd.run(GRAB_ABI_PATH + " " + address);
  var abiFile = getABIFile(address)
  if (fs.existsSync(abiFile)) {
    var abiJSONString = read.sync(abiFile, 'utf8');
    var abiJSON = JSON.parse(abiJSONString)
    response.json(abiJSON)
  }
  else {
    response.json({
      "error": true
    })
  }
});

app.get("/decode_params/:inputs", function(request, response) {
  inputs_array = request.params.inputs.split(",")
  if (decoder.decodeMethod(inputs_array[0])) {
    var json = inputs_array.map(input => decoder.decodeMethod(input))
    response.json(json)
  } else response.json({'contract_without_abi': true})
});

app.listen(3000, function () {
  console.log('Server listening on port 3000');
});
