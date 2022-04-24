const constants = require('./constants');

// Constrain query cost
const ONE_GB_IN_BYTES = 1073741824;
// Assuming the price of query is $0.01 per GB
const QUERY_COST_PER_GB_IN_DOLLARS = 0.01;
// Query cost limit
const QUERY_COST_LIMIT = 0.1;

// Get the 15 most recently added data points in the past 15 minutes. 
// You can change the time period if you're not continuously ingesting data
const QUERY_1 = "SELECT * FROM " + constants.DATABASE_NAME + "." +  constants.TABLE_NAME +
                " WHERE time between ago(15m) and now() ORDER BY time DESC LIMIT 15";

async function runAllQueries() {
    const queries = [QUERY_1];

    for (let i = 0; i < queries.length; i++) {
        console.log(`Running query ${i+1} : ${queries[i]}`);
        await getAllRows(queries[i], null);
    }
}

async function getAllRows(query, nextToken = undefined) {
    let response;
    try {
        response = await queryClient.query(params = {
            QueryString: query,
            NextToken: nextToken,
        }).promise();
    } catch (err) {
        console.error("Error while querying:", err);
        throw err;
    }

    parseQueryResult(response);
    if (response.NextToken) {
        await getAllRows(query, response.NextToken);
    }
}

function parseQueryResult(response) {
    const queryStatus = response.QueryStatus;
    console.log("Current query status: " + JSON.stringify(queryStatus));
    var bytesMetered = queryStatus["CumulativeBytesMetered"] / ONE_GB_IN_BYTES
    console.log("Bytes Metered so far: " + bytesMetered + " GB");
    var queryCost = bytesMetered * QUERY_COST_PER_GB_IN_DOLLARS;
    console.log("Query cost: " + queryCost)
    if( queryCost> QUERY_COST_LIMIT)
        throw new Error("Query cost over the limit!")

    const columnInfo = response.ColumnInfo;
    const rows = response.Rows;

    console.log("Metadata: " + JSON.stringify(columnInfo));
    console.log("Data: ");

    rows.forEach(function (row) {
        console.log(parseRow(columnInfo, row));
    });
}

function parseRow(columnInfo, row) {
    const data = row.Data;
    const rowOutput = [];

    var i;
    for ( i = 0; i < data.length; i++ ) {
        info = columnInfo[i];
        datum = data[i];
        rowOutput.push(parseDatum(info, datum));
    }

    return `{${rowOutput.join(", ")}}`
}

function parseDatum(info, datum) {
    if (datum.NullValue != null && datum.NullValue === true) {
        return `${info.Name}=NULL`;
    }

    const columnType = info.Type;

    // If the column is of TimeSeries Type
    if (columnType.TimeSeriesMeasureValueColumnInfo != null) {
        return parseTimeSeries(info, datum);
    }
    // If the column is of Array Type
    else if (columnType.ArrayColumnInfo != null) {
        const arrayValues = datum.ArrayValue;
        return `${info.Name}=${parseArray(info.Type.ArrayColumnInfo, arrayValues)}`;
    }
    // If the column is of Row Type
    else if (columnType.RowColumnInfo != null) {
        const rowColumnInfo = info.Type.RowColumnInfo;
        const rowValues = datum.RowValue;
        return parseRow(rowColumnInfo, rowValues);
    }
    // If the column is of Scalar Type
    else {
        return parseScalarType(info, datum);
    }
}

function parseTimeSeries(info, datum) {
    const timeSeriesOutput = [];
    datum.TimeSeriesValue.forEach(function (dataPoint) {
        timeSeriesOutput.push(`{time=${dataPoint.Time}, value=${parseDatum(info.Type.TimeSeriesMeasureValueColumnInfo, dataPoint.Value)}}`)
    });

    return `[${timeSeriesOutput.join(", ")}]`
}

function parseScalarType(info, datum) {
    return parseColumnName(info) + datum.ScalarValue;
}

function parseColumnName(info) {
    return info.Name == null ? "" : `${info.Name}=`;
}

function parseArray(arrayColumnInfo, arrayValues) {
    const arrayOutput = [];
    arrayValues.forEach(function (datum) {
        arrayOutput.push(parseDatum(arrayColumnInfo, datum));
    });
    return `[${arrayOutput.join(", ")}]`
}

module.exports = {runAllQueries, getAllRows};

