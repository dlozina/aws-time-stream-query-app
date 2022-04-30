const queryParser = require("../utils/query-parser");
const constants = require("../constants");

//Get the 15 most recently added data points in the past 15 minutes.


async function queryService() {

    const QUERY = "SELECT * FROM " + constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " WHERE time between ago(15m) and now() ORDER BY time DESC LIMIT 15";

    return await queryParser.getAllRows(QUERY);
    // Try a query with multiple pages
    //await queryExample.tryQueryWithMultiplePages(20000);

    //Try cancelling a query
    //This could fail if there is no data in the table,
    //and the example query has finished before it was cancelled.
    //await queryExample.tryCancelQuery();
}

module.exports = {queryService};