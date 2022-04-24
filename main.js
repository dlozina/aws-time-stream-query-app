// import entire SDK
var AWS = require("aws-sdk");

// Loading Examples
//const queryExample = require("./query-example");
const queryTest = require("./query-custom");

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

async function callServices() {
    
    await queryTest.runAllQueries();

    // Try a query with multiple pages
    //await queryExample.tryQueryWithMultiplePages(20000);

    //Try cancelling a query
    //This could fail if there is no data in the table,
    //and the example query has finished before it was cancelled.
    //await queryExample.tryCancelQuery();
}

callServices();
