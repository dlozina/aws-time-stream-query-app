const AWS = require("aws-sdk");
const express = require('express')
const services = require("./services/query-service")

const app = express();
app.use(express.json())
// Enable CORS for all methods
app.use(function(req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "*");
    next();
});

AWS.config.loadFromPath('./config.json');
// Check credentials
AWS.config.getCredentials(function(err) {
    if (err) {
        console.log(err.stack);
    }
    else {
      console.log("Access key:", AWS.config.credentials.accessKeyId);
    }
  });
// Configuring AWS SDK
AWS.config.update({ region: "us-east-1" });

// Check region
console.log("Region: ", AWS.config.region);

// Creating Timestream Query client
queryClient = new AWS.TimestreamQuery();

app.get('/recently-added-data', async function(req, res) {
    let result = await services.queryService();

    res.json(result);
});

app.listen(3000, function() {
    console.log("App started")
});

module.exports = app


