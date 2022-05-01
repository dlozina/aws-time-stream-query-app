// Constrain query cost
const ONE_GB_IN_BYTES = 1073741824;
// Assuming the price of query is $0.01 per GB
const QUERY_COST_PER_GB_IN_DOLLARS = 0.01;
// Query cost limit
const QUERY_COST_LIMIT = 0.1;

/**
 * Get and parse all query results.
 * @param {string} query - Query request.
 * @param {string} nextToken - Add next query.
 */
async function getAllRows(query, nextToken = undefined) {
  let response;
  try {
    response = await queryClient.query(params = {
      QueryString: query,
      NextToken: nextToken,
    }).promise();
  } catch (err) {
    console.error('Error while querying:', err);
    throw err;
  }

  const results = parseQueryResult(response);
  if (response.NextToken) {
    await getAllRows(query, response.NextToken);
  }

  return results;
}

/**
 * Get metadata, column and row information.
 * @param {string} response - Query response.
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

  return arrayResults;
}

/**
 * Parse every result row.
 * @param {string} columnInfo - Column from query response.
 * @param {string} row - Row from query response.
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
 * @param {string} info - Column from query response.
 * @param {string} datum - Row from query response.
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
 * @param {string} info - Column from query response.
 * @param {string} datum - Row from query response.
 * @return {string} - String
 */
function parseTimeSeries(info, datum) {
  const timeSeriesOutput = [];
  datum.TimeSeriesValue.forEach(function(dataPoint) {
    // eslint-disable-next-line max-len
    timeSeriesOutput.push(`{time=${dataPoint.Time}, value=${parseDatum(info.Type.TimeSeriesMeasureValueColumnInfo, dataPoint.Value)}}`);
  });

  return `[${timeSeriesOutput.join(', ')}]`;
}

/**
 * Parse parseScalarType.
 * @param {string} info - Column from query response.
 * @param {string} datum - Row from query response.
 * @return {{}} - Return Object
 */
function parseScalarType(info, datum) {
  const column = parseColumnName(info);
  return {
    [column]: datum.ScalarValue,
  };
}

/**
 * Parse Column Name.
 * @param {string} info - Column from query response.
 * @param {string} datum - Row from query response.
 * @return {string} - Return String
 */
function parseColumnName(info) {
  return info.Name == null ? '' : `${info.Name}`;
}

/**
 * Parse Array.
 * @param {string} arrayColumnInfo - Column from query response.
 * @param {[]} arrayValues - Row from query response.
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

