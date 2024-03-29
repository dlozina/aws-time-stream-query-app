// Constrain query cost
const ONE_GB_IN_BYTES = 1073741824;
// Assuming the price of query is $0.01 per GB
const QUERY_COST_PER_GB_IN_DOLLARS = 0.01;
// Query cost limit
const QUERY_COST_LIMIT = 0.1;

/**
 * Get and parse all query results.
 * Timestream returns the result set in a paginated manner to optimize the
 * responsiveness of your applications
 * @param {string} query - Query request.
 * @param {string} nextToken - Pagination token.
 * @return {[]} - Return array of objects
 */
async function getAllRows(query) {
  let response;
  let results;
  try {
    response = await queryClient.query(params = {
      QueryString: query,
    }).promise();
  } catch (err) {
    console.error('Error while querying:', err);
    throw new Error('Error in query string');
  }

  results = parseQueryResult(response);
  if (response.NextToken) {
    results = await getAllRowsWithPaging(query, response.NextToken);
  }

  return results;
}

/**
 * Get and parse all query results with NextToken.
 * Timestream returns the result set in a paginated manner to optimize the
 * responsiveness of your applications
 * @param {string} query - Query request.
 * @param {string} nextToken - Pagination token.
 * @return {[]} - Return array of objects
 */
async function getAllRowsWithPaging(query, nextToken = undefined) {
  let response;
  try {
    response = await queryClient.query(params = {
      QueryString: query,
      NextToken: nextToken,
      MaxRows: 30,
    }).promise();
  } catch (err) {
    console.error('Error while querying:', err);
    throw new Error('Error in query string');
  }

  return parseQueryResult(response);
}

/**
 * Get metadata, column and row information.
 * @param {{}} response - Query response timestream object.
 * @return {[]} - Array of results
 */
function parseQueryResult(response) {
  const queryStatus = response.QueryStatus;
  console.log('Current query status: ' + JSON.stringify(queryStatus));
  const bytesMetered = queryStatus['CumulativeBytesMetered'] / ONE_GB_IN_BYTES;
  console.log('Bytes Metered so far: ' + bytesMetered + ' GB');
  const queryCost = bytesMetered * QUERY_COST_PER_GB_IN_DOLLARS;
  console.log('Query cost: ' + queryCost);
  if ( queryCost> QUERY_COST_LIMIT) {
    throw new Error('Query cost over the limit!');
  }

  const columnInfo = response.ColumnInfo;
  const rows = response.Rows;

  // Get Metadata
  // console.log("Metadata: " + JSON.stringify(columnInfo));

  const arrayResults = [];
  rows.forEach(function(row) {
    arrayResults.push(parseRow(columnInfo, row));
  });

  const postProcessingArrayResults = [];
  let i;
  for (i = 0; i < arrayResults.length; i++) {
    const editedResult = Object.assign({}, ...arrayResults[i]);
    postProcessingArrayResults.push(editedResult);
  }

  return postProcessingArrayResults;
}

/**
 * Parse every result row.
 * @param {[]} columnInfo - Column array from query response.
 * @param {{}} row - Row object from query response.
 * @return {[]} - Array of results
 */
function parseRow(columnInfo, row) {
  const data = row.Data;
  const rowOutput = [];

  let i;
  let info;
  let datum;
  for (i = 0; i < data.length; i++) {
    info = columnInfo[i];
    datum = data[i];
    rowOutput.push(parseDatum(info, datum));
  }
  return rowOutput;
}

/**
 * Parse every result row.
 * @param {{}} info - Single Column object from query response.
 * @param {{}} datum - Single Row object from query response.
 * @return {{}} - Return object
 */
function parseDatum(info, datum) {
  if (datum.NullValue != null && datum.NullValue === true) {
    return {
      [info.Name]: 'NULL',
    };
  }

  const columnType = info.Type;

  if (columnType.TimeSeriesMeasureValueColumnInfo != null) {
    // If the column is of TimeSeries Type
    return parseTimeSeries(info, datum);
  } else if (columnType.ArrayColumnInfo != null) {
    // If the column is of Array Type
    const arrayValues = datum.ArrayValue;
    return `${info.Name}=${parseArray(info.Type.ArrayColumnInfo, arrayValues)}`;
  } else if (columnType.RowColumnInfo != null) {
    // If the column is of Row Type
    const rowColumnInfo = info.Type.RowColumnInfo;
    const rowValues = datum.RowValue;
    return parseRow(rowColumnInfo, rowValues);
  } else {
    // If the column is of Scalar Type
    return parseScalarType(info, datum);
  }
}

/**
 * Parse parseTimeSeries.
 * @param {{}} info - Column info object
 * @param {{}} datum - Timeseries object
 * @return {[]} - String
 */
function parseTimeSeries(info, datum) {
  const timeSeriesOutput = [];
  datum.TimeSeriesValue.forEach(function(dataPoint) {
    const value = parseDatum(
        info.Type.TimeSeriesMeasureValueColumnInfo,
        dataPoint.Value);
    const time = dataPoint.Time;
    timeSeriesOutput.push({['time']: time, ['value']: value['scalarValue']});
  });

  return timeSeriesOutput;
}

/**
 * Parse parseScalarType.
 * @param {{}} info - Column info object
 * @param {{}} datum - Scalar value object
 * @return {{}} - Return field object
 */
function parseScalarType(info, datum) {
  let column = parseColumnName(info);
  // Added check for TimeSeries scalar values
  if (column === '') {
    column = 'scalarValue';
  }
  return {
    [column]: datum.ScalarValue,
  };
}

/**
 * Parse Column Name.
 * @param {{}} info - Column info object
 * @param {string} datum - Row from query response
 * @return {string} - Return String
 */
function parseColumnName(info) {
  return info.Name == null ? '' : `${info.Name}`;
}

/**
 * Parse Array.
 * @param {{}} arrayColumnInfo - Column info object
 * @param {[]} arrayValues - Row array values
 * @return {string} - Return string
 */
function parseArray(arrayColumnInfo, arrayValues) {
  const arrayOutput = [];
  arrayValues.forEach(function(datum) {
    arrayOutput.push(parseDatum(arrayColumnInfo, datum));
  });
  return `[${arrayOutput.join(', ')}]`;
}

module.exports = {getAllRows};

