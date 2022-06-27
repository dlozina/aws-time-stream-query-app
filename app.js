const AWS = require('aws-sdk');
const express = require('express');
const services = require('./services/query-service');

const app = express();
app.use(express.json());
// *** Enable CORS for all methods ***
app.use(function(req, res, next) {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', '*');
  next();
});

AWS.config.loadFromPath('./config.json');
// *** Check credentials ***
AWS.config.getCredentials(function(err) {
  if (err) {
    console.log(err.stack);
  } else {
    console.log('Access key:', AWS.config.credentials.accessKeyId);
  }
});
// *** Configuring AWS SDK ***
AWS.config.update({region: 'us-east-1'});

// *** Check region ***
console.log('Region: ', AWS.config.region);

//  *** Creating Timestream Query client ***
queryClient = new AWS.TimestreamQuery();

// *** App endpoints ***
app.get('/recently-added-data', async function(req, res) {
  try {
    const result = await services.recentlyAddedData();
    res.status(200).json(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/recently-added-data/:devEui', async function(req, res) {
  try {
    const result = await services.recentlyAddedDataFromDevice(
        req.params.devEui);
    res.status(200).json(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/hourly-consumption/:devEui', async function(req, res) {
  try {
    const result = await services.hourlyConsumptionFromDevice(
        req.params.devEui);
    res.status(200).json(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/daily-consumption/:devEui', async function(req, res) {
  try {
    const result = await services.dailyConsumptionFromDevice(req.params.devEui);
    res.status(200).json(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/weekly-consumption/:devEui', async function(req, res) {
  try {
    const result = await services.weeklyConsumptionFromDevice(
        req.params.devEui);
    res.status(200).json(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/monthly-consumption/:devEui', async function(req, res) {
  try {
    const result = await services.monthlyConsumptionFromDevice(
        req.params.devEui);
    res.status(200).json(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.post('/consumption-period/:devEui', async function(req, res) {
  try {
    const result = await services.consumptionPeriodFromDevice(
        req.params.devEui,
        req.body.sinceDatetime,
        req.body.untilDatetime,
    );
    res.status(200).json(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/hourly-temp-graph/:devEui', async function(req, res) {
  try {
    const result = await services.hourlyTempGraph(req.params.devEui);
    res.status(200).json(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/table-data', async function(req, res) {
  try {
    const result = await services.tableData();
    res.status(200).json(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/change-table-data', async function(req, res) {
  try {
    const result = await services.changeTableData();
    res.status(200).json(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.post('/full-table-data', async function(req, res) {
  try {
    const result = await services.fullTableDataV2(req.body.devEuis);
    res.status(200).json(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.get('/table-data-test', async function(req, res) {
  try {
    const result = [{
      devEui: 'MyDevice',
      changeOverDay: 1,
      changeOverWeek: 2,
      changeOverMonth: 3,
      temperature: 28,
    },
    {
      devEui: 'MyDevice',
      changeOverDay: 2,
      changeOverWeek: 2,
      changeOverMonth: 2,
      temperature: 30,
    },
    ];
    res.status(200).json(result);
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.listen(3000, function() {
  console.log('App started');
});

module.exports = app;


