var decoder = new require('abi-decoder')
var express = require('express');
var app = express();
var read = require('read-file');
var cmd = require('node-run-cmd');
var fs = require('fs');
var bodyParser = require('body-parser');

app.use(bodyParser.json());

app.post("/add_abi", function(request, response) {
  decoder.addABI(request.body)
  response.json({
    "added": true
  })
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
