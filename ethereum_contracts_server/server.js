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

app.get("/add_abi/:address", function(request, response) {
  var address = request.params.address
  cmd.run(GRAB_ABI_PATH + " " + address);
  var abiFile = getABIFile(address)
  if (fs.existsSync(abiFile)) {
    var abiJSONString = read.sync(abiFile, 'utf8');
    var abiJSON = JSON.parse(abiJSONString)
    decoder.addABI(abiJSON)
    response.json({
      "success": true
    })
  }
  else {
    response.json({
      "success": false
    })
  }
});

app.get("/decode_params/:method", function(request, response) {
  var json = decoder.decodeMethod(request.params.method)
  response.json(json)
});

app.listen(3000, function () {
  console.log('Server listening on port 3000');
});
