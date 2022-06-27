const queryParser = require('../utils/query-parser');
const constants = require('../constants');

/**
 * Run query to get results.
 * Get the 15 most recently added data points in the past 15 minutes.
 * @return {[]} - Array of results
 */
async function recentlyAddedData() {
  const QUERY = 'SELECT * FROM ' + constants.DATABASE_NAME +
      '.' + constants.TABLE_NAME +
      ' WHERE time between ago(15m) and now() ORDER BY time DESC LIMIT 15';

  return await queryParser.getAllRows(QUERY);
  // Try a query with multiple pages
  // await queryExample.tryQueryWithMultiplePages(20000);

  // Try cancelling a query
  // This could fail if there is no data in the table,
  // and the example query has finished before it was cancelled.
  // await queryExample.tryCancelQuery();
}

/**
 * Run query to get results.
 * Get the 15 most recently added data points,
 * from specific device.
 * @param {string} devEui - DevEui for query
 * @return {[]} - Array of results
 */
async function recentlyAddedDataFromDevice(devEui) {
  const QUERY = "SELECT * FROM " + constants.DATABASE_NAME +
      "." + constants.TABLE_NAME +
      " WHERE devEui = '" + devEui + "' ORDER BY time DESC LIMIT 15";

  return await queryParser.getAllRows(QUERY);
  // Try a query with multiple pages
  // await queryExample.tryQueryWithMultiplePages(20000);

  // Try cancelling a query
  // This could fail if there is no data in the table,
  // and the example query has finished before it was cancelled.
  // await queryExample.tryCancelQuery();
}

/**
 * Run query to get results.
 * Get consumption in last 24h,
 * from specific device.
 * @param {string} devEui - DevEui for query
 * @return {[[{string}]]} - String Result in array (Specific parser needed)
 */
async function hourlyConsumptionFromDevice(devEui) {
  const QUERY = "SELECT ROUND(((SELECT " +
      "ROUND(MAX(totalGallons),3) AS max_value_total_gallons " +
      "FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE devEui = '" + devEui + "' AND time >= ago(1h))" +
      "-" +
      "(SELECT " +
      "ROUND(MIN(totalGallons),3) AS min_value_total_gallons " +
      "FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE devEui = '" + devEui + "' AND time >= ago(1h))" +
      "), 3) as hourly_consumption"

  return await queryParser.getAllRows(QUERY);
}

/**
 * Run query to get results.
 * Get consumption in last 24h,
 * from specific device.
 * @param {string} devEui - DevEui for query
 * @return {[[{string}]]} - String Result in array (Specific parser needed)
 */
async function dailyConsumptionFromDevice(devEui) {
  const QUERY = "SELECT ROUND(((SELECT " +
      "ROUND(MAX(totalGallons),3) AS max_value_total_gallons " +
      "FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE devEui = '" + devEui + "' AND time >= ago(24h))" +
      "-" +
      "(SELECT " +
      "ROUND(MIN(totalGallons),3) AS min_value_total_gallons " +
      "FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE devEui = '" + devEui + "' AND time >= ago(24h))" +
      "), 3) as daily_consumption"

  return await queryParser.getAllRows(QUERY);
}

/**
 * Run query to get results.
 * Get consumption in last 7 days,
 * from specific device.
 * @param {string} devEui - DevEui for query
 * @return {[[{string}]]} - String Result in array (Specific parser needed)
 */
async function weeklyConsumptionFromDevice(devEui) {
  const QUERY = "SELECT ROUND(((SELECT " +
      "ROUND(MAX(totalGallons),3) AS max_value_total_gallons " +
      "FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE devEui = '" + devEui + "' AND time >= ago(7d))" +
      "-" +
      "(SELECT " +
      "ROUND(MIN(totalGallons),3) AS min_value_total_gallons " +
      "FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE devEui = '" + devEui + "' AND time >= ago(7d))" +
      "), 3) as weekly_consumption"

  return await queryParser.getAllRows(QUERY);
}

/**
 * Run query to get results.
 * Get consumption in last 30 days,
 * from specific device.
 * @param {string} devEui - DevEui for query
 * @return {[[{string}]]} - String Result in array (Specific parser needed)
 */
async function monthlyConsumptionFromDevice(devEui) {
  const QUERY = "SELECT ROUND(((SELECT " +
      "ROUND(MAX(totalGallons),3) AS max_value_total_gallons " +
      "FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE devEui = '" + devEui + "' AND time >= ago(30d))" +
      "-" +
      "(SELECT " +
      "ROUND(MIN(totalGallons),3) AS min_value_total_gallons " +
      "FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE devEui = '" + devEui + "' AND time >= ago(30d))" +
      "), 3) as monthly_consumption"

  return await queryParser.getAllRows(QUERY);
}

/**
 * Run query to get results.
 * Get consumption in time period,
 * from specific device.
 * @param {string} devEui - DevEui for query
 * @param {string} sinceDatetime - Restrict the query range to data samples
 * since this datetime
 @param {string} untilDatetime - Restrict the query range to data samples
 until this datetime
 * @return {[[{string}]]} - String Result in array (Specific parser needed)
 */
async function consumptionPeriodFromDevice(
    devEui,
    sinceDatetime,
    untilDatetime) {
  const QUERY = "SELECT ROUND(((SELECT " +
      "ROUND(MAX(totalGallons),3) AS max_value_total_gallons " +
      "FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE devEui = '" + devEui + "' AND time BETWEEN TIMESTAMP '" +
      sinceDatetime + "' AND TIMESTAMP '" + untilDatetime + "')" +
      "-" +
      "(SELECT " +
      "ROUND(MIN(totalGallons),3) AS min_value_total_gallons " +
      "FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE devEui = '" + devEui +
      "' AND time BETWEEN TIMESTAMP '" + sinceDatetime + "' AND TIMESTAMP '" +
      untilDatetime + "')), 3) as period_consumption"

  return await queryParser.getAllRows(QUERY);
}

/**
 * Run query to get results.
 * Get hourly temperature graph,
 * from specific device.
 * @param {string} devEui - DevEui for query
 * @return {[[{string}]]} - String Result in array (Specific parser needed)
 */
async function hourlyTempGraph(devEui) {
  const QUERY = "WITH data AS (" +
      "SELECT BIN(time, 5m) as t ,devEui ,temperatureC AS temperature" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE devEui = '" + devEui + "' AND time BETWEEN ago(1h) AND now()" +
      " GROUP BY BIN(time, 5m), devEui, temperatureC) " +
      "SELECT devEui," +
      " INTERPOLATE_LINEAR(CREATE_TIME_SERIES(t, temperature)," +
      " SEQUENCE(min(t), max(t), 10m)) AS interpolated_temperature" +
      " FROM data" +
      " GROUP BY devEui" +
      " ORDER BY devEui"

  return await queryParser.getAllRows(QUERY);
}

/**
 * Run query to get results.
 * Get table data
 * @return {[[{string}]]} - String Result in array (Specific parser needed)
 */
async function tableData() {
  const QUERY = "WITH consumption_24_hours AS (" +
      "SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS changeOverDay" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE time >= ago(24h) GROUP BY devEui )," +
      " consumption_7_days AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3) " +
      " AS changeOverWeek" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE time >= ago(7d) GROUP BY devEui )," +
      " consumption_30_days AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3) " +
      " AS changeOverMonth" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE time >= ago(30d) GROUP BY devEui )," +
      " latest_recorded_time AS (" +
      " SELECT devEui, max(time) as latest_time" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE time >= ago(24h) GROUP BY devEui )," +
      " recent_temperature_reading AS (" +
      " SELECT b.devEui, b.temperatureC as temperature" +
      " FROM latest_recorded_time a INNER JOIN " +
      constants.DATABASE_NAME + "." + constants.TABLE_NAME + " b" +
      " ON a.devEui = b.devEui AND b.time = a.latest_time" +
      " WHERE b.time > ago(24h) ORDER BY b.devEui )" +
      " SELECT b.devEui, changeOverDay, changeOverWeek," +
      " changeOverMonth, temperature" +
      " FROM consumption_24_hours a INNER JOIN consumption_7_days b" +
      " ON a.devEui = b.devEui INNER JOIN consumption_30_days c" +
      " ON a.devEui = c.devEui INNER JOIN recent_temperature_reading d" +
      " ON a.devEui = d.devEui ORDER BY b.devEui"

  return await queryParser.getAllRows(QUERY);
}

/**
 * Run query to get results.
 * Get table data
 * @return {[[{string}]]} - String Result in array (Specific parser needed)
 */
async function changeTableData() {
  const QUERY = "WITH consumption_1_day AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumptionInDayPeriod" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE time >= ago(1d) GROUP BY devEui )," +
      " consumption_1_day_before AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumptionInDayPeriodBefore" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE time < ago(1d) AND time >= ago(2d) GROUP BY devEui ),"+
      " consumption_7_days AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumptionInWeekPeriod" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE time >= ago(7d) GROUP BY devEui )," +
      " consumption_7_days_before AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumptionInWeekPeriodBefore" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE time < ago(7d) AND time >= ago(14d) GROUP BY devEui )," +
      " consumption_30_days AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumptionInMonthPeriod" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE time >= ago(30d) GROUP BY devEui )," +
      " consumption_30_days_before AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumptionInMonthPeriodBefore" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE time < ago(30d) AND time >= ago(60d) GROUP BY devEui )" +
      " SELECT b.devEui, consumptionInDayPeriod AS dayChange," +
      " consumptionInDayPeriodBefore AS dayBeforeChange," +
      " consumptionInWeekPeriod AS weekChange," +
      " consumptionInWeekPeriodBefore AS weekBeforeChange," +
      " consumptionInMonthPeriod AS monthChange," +
      " consumptionInMonthPeriodBefore AS monthBeforeChange" +
      " FROM consumption_1_day a JOIN consumption_1_day_before b" +
      " ON a.devEui = b.devEui" +
      " JOIN consumption_7_days c ON b.devEui = c.devEui" +
      " JOIN consumption_7_days_before d ON c.devEui = d.devEui" +
      " JOIN consumption_30_days e ON d.devEui = e.devEui" +
      " JOIN consumption_30_days_before f ON e.devEui = f.devEui" +
      " ORDER BY b.devEui"

  return await queryParser.getAllRows(QUERY);
}

/**
 * Run query to get results.
 * Get table data
 * @param {[]} devEuis - DevEuis for query
 * @return {[[{string}]]} - String Result in array (Specific parser needed)
 */
async function fullTableData(devEuis) {
  devEuis.forEach(preProcessing);
  const devices = devEuis.join(' OR ');
  const QUERY = "WITH get_data_last_two_months AS (" +
      " SELECT time, devEui, totalGallons, temperatureC" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE (time >= ago(60d)) AND (" + devices + ") ORDER BY time )," +
      " get_selected_devices AS (" +
      " SELECT DISTINCT(devEui), null AS consumption" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE " + devices + ")," +
      " consumption_1_day AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumption" +
      " FROM get_data_last_two_months" +
      " WHERE (time >= ago(24h)) "+
      " GROUP BY devEui )," +
      " consumption_1_day_table AS (" +
      " SELECT a.devEui, b.consumption" +
      " FROM get_selected_devices a LEFT JOIN consumption_1_day b" +
      " ON a.devEui = b.devEui)," +
      " consumption_1_day_before AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumption" +
      " FROM get_data_last_two_months"  +
      " WHERE (time < ago(1d) AND time >= ago(2d))" +
      " GROUP BY devEui )," +
      " consumption_1_day_before_table AS (" +
      " SELECT a.devEui, b.consumption" +
      " FROM get_selected_devices a LEFT JOIN consumption_1_day_before b" +
      " ON a.devEui = b.devEui)," +
      " consumption_7_days AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumption" +
      " FROM get_data_last_two_months"  +
      " WHERE (time >= ago(7d))" +
      " GROUP BY devEui )," +
      " consumption_7_days_table AS (" +
      " SELECT a.devEui, b.consumption" +
      " FROM get_selected_devices a LEFT JOIN consumption_7_days b" +
      " ON a.devEui = b.devEui)," +
      " consumption_7_days_before AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumption" +
      " FROM get_data_last_two_months" +
      " WHERE (time < ago(7d) AND time >= ago(14d))" +
      " GROUP BY devEui )," +
      " consumption_7_days_before_table AS (" +
      " SELECT a.devEui, b.consumption" +
      " FROM get_selected_devices a LEFT JOIN consumption_7_days_before b" +
      " ON a.devEui = b.devEui)," +
      " consumption_30_days AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumption" +
      " FROM get_data_last_two_months" +
      " WHERE (time >= ago(30d))" +
      " GROUP BY devEui )," +
      " consumption_30_days_table AS (" +
      " SELECT a.devEui, b.consumption" +
      " FROM get_selected_devices a LEFT JOIN consumption_30_days b" +
      " ON a.devEui = b.devEui)," +
      " consumption_30_days_before AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumption" +
      " FROM get_data_last_two_months" +
      " WHERE (time < ago(30d) AND time >= ago(60d))" +
      " GROUP BY devEui )," +
      " consumption_30_days_before_table AS (" +
      " SELECT a.devEui, b.consumption" +
      " FROM get_selected_devices a LEFT JOIN consumption_30_days_before b" +
      " ON a.devEui = b.devEui)," +
      " latest_recorded_time AS (" +
      " SELECT devEui, max(time) as latest_time" +
      " FROM get_data_last_two_months" +
      " WHERE (time >= ago(24h))" +
      " GROUP BY devEui )," +
      " recent_temperature_reading AS (" +
      " SELECT  b.devEui, b.temperatureC as consumption" +
      " FROM latest_recorded_time a INNER JOIN " +
      " get_data_last_two_months b" +
      " ON a.devEui = b.devEui AND b.time = a.latest_time" +
      " WHERE b.time > ago(24h))," +
      " recent_temperature_reading_table AS (" +
      " SELECT a.devEui, b.consumption as lastReportedTemperatureReading" +
      " FROM get_selected_devices a LEFT JOIN recent_temperature_reading b" +
      " ON a.devEui = b.devEui)" +
      " SELECT b.devEui, a.consumption as consumptionInDay," +
      " ROUND((a.consumption - b.consumption)/b.consumption * 100, 2)" +
      " AS changePercentageDay," +
      " c.consumption as consumptionInWeek," +
      " ROUND((c.consumption - d.consumption)/d.consumption * 100, 2)" +
      " AS changePercentageWeek," +
      " e.consumption as consumptionInMonth," +
      " ROUND((e.consumption - f.consumption)/f.consumption * 100, 2)" +
      " AS changePercentageMonth," +
      " lastReportedTemperatureReading" +
      " FROM" +
      " consumption_1_day_table a INNER JOIN consumption_1_day_before_table b" +
      " ON a.devEui = b.devEui" +
      " INNER JOIN consumption_7_days_table c ON b.devEui = c.devEui" +
      " INNER JOIN consumption_7_days_before_table d ON c.devEui = d.devEui" +
      " INNER JOIN consumption_30_days_table e ON d.devEui = e.devEui" +
      " INNER JOIN consumption_30_days_before_table f ON e.devEui = f.devEui" +
      " INNER JOIN recent_temperature_reading_table g ON f.devEui = g.devEui"

  return await queryParser.getAllRows(QUERY);
}

/**
 * Run query two queries in parallel to get faster response time.
 * Get table data
 * @param {[]} devEuis - DevEuis for query
 * @return {[[{string}]]} - String Result in array (Specific parser needed)
 */
async function fullTableDataV2(devEuis) {
  devEuis.forEach(preProcessing);
  const devices = devEuis.join(' OR ');

  // Get Day and Week data
  const QUERY1 = "WITH get_selected_devices AS (" +
      " SELECT devEui, null AS consumption" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE " + devices + " GROUP BY devEui )," +
      " consumption_1_day AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumption" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE (time >= ago(24h)) AND (" + devices + ")" +
      " GROUP BY devEui )," +
      " consumption_1_day_table AS (" +
      " SELECT a.devEui, b.consumption" +
      " FROM get_selected_devices a LEFT JOIN consumption_1_day b" +
      " ON a.devEui = b.devEui ORDER BY devEui )," +
      " consumption_1_day_before AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumption" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE (time < ago(1d) AND time >= ago(2d)) AND (" + devices + ")" +
      " GROUP BY devEui )," +
      " consumption_1_day_before_table AS (" +
      " SELECT a.devEui, b.consumption" +
      " FROM get_selected_devices a LEFT JOIN consumption_1_day_before b" +
      " ON a.devEui = b.devEui ORDER BY devEui )," +
      " consumption_7_days AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumption" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE (time >= ago(7d)) AND (" + devices + ")" +
      " GROUP BY devEui )," +
      " consumption_7_days_table AS (" +
      " SELECT a.devEui, b.consumption" +
      " FROM get_selected_devices a LEFT JOIN consumption_7_days b" +
      " ON a.devEui = b.devEui ORDER BY devEui )," +
      " consumption_7_days_before AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumption" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE (time < ago(7d) AND time >= ago(14d)) AND (" + devices + ")" +
      " GROUP BY devEui )," +
      " consumption_7_days_before_table AS (" +
      " SELECT a.devEui, b.consumption" +
      " FROM get_selected_devices a LEFT JOIN consumption_7_days_before b" +
      " ON a.devEui = b.devEui ORDER BY devEui )," +
      " latest_recorded_time AS (" +
      " SELECT devEui, max(time) as latest_time" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      "  WHERE (time >= ago(24h)) AND (" + devices + ")" +
      " GROUP BY devEui )," +
      " recent_temperature_reading AS (" +
      " SELECT b.devEui, b.temperatureC as consumption" +
      " FROM latest_recorded_time a INNER JOIN " +
      constants.DATABASE_NAME + "." + constants.TABLE_NAME + " b" +
      " ON a.devEui = b.devEui AND b.time = a.latest_time" +
      " WHERE b.time > ago(24h) ORDER BY b.devEui )," +
      " recent_temperature_reading_table AS (" +
      " SELECT a.devEui, b.consumption as lastReportedTemperatureReading" +
      " FROM get_selected_devices a LEFT JOIN recent_temperature_reading b" +
      " ON a.devEui = b.devEui ORDER BY devEui )" +
      " SELECT a.devEui, a.consumption as consumptionInDay," +
      "  ROUND((a.consumption - b.consumption)/b.consumption * 100, 2)" +
      " AS changePercentageDay," +
      " c.consumption as consumptionInWeek," +
      " ROUND((c.consumption - d.consumption)/d.consumption * 100, 2)" +
      " AS changePercentageWeek," +
      " lastReportedTemperatureReading" +
      " FROM" +
      " consumption_1_day_table a INNER JOIN consumption_1_day_before_table b" +
      " ON a.devEui = b.devEui" +
      " INNER JOIN consumption_7_days_table c" +
      " ON b.devEui = c.devEui" +
      " INNER JOIN consumption_7_days_before_table d" +
      " ON c.devEui = d.devEui" +
      " INNER JOIN recent_temperature_reading_table e" +
      " ON d.devEui = e.devEui" +
      " ORDER BY a.devEui"


  // Get Month data
  const QUERY2 = "WITH get_selected_devices AS (" +
      " SELECT devEui, null AS consumption" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE " + devices + " GROUP BY devEui )," +
      " consumption_30_days AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumption" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE (time >= ago(30d)) AND (" + devices + ")" +
      " GROUP BY devEui )," +
      " consumption_30_days_table AS (" +
      " SELECT a.devEui, b.consumption" +
      " FROM get_selected_devices a LEFT JOIN consumption_30_days b" +
      " ON a.devEui = b.devEui ORDER BY devEui )," +
      " consumption_30_days_before AS (" +
      " SELECT devEui, ROUND(MAX(totalGallons) - MIN(totalGallons),3)" +
      " AS consumption" +
      " FROM " + constants.DATABASE_NAME + "." + constants.TABLE_NAME +
      " WHERE (time < ago(30d) AND time >= ago(60d)) AND (" + devices + ")" +
      " GROUP BY devEui )," +
      " consumption_30_days_before_table AS ("+
      " SELECT a.devEui, b.consumption" +
      " FROM get_selected_devices a LEFT JOIN consumption_30_days_before b" +
      " ON a.devEui = b.devEui ORDER BY devEui )" +
      " SELECT a.devEui, a.consumption as consumptionInMonth," +
      " ROUND((a.consumption - b.consumption)/b.consumption * 100, 2)" +
      " AS changePercentageMonth" +
      " FROM consumption_30_days_table a" +
      " INNER JOIN consumption_30_days_before_table b" +
      " ON a.devEui = b.devEui ORDER BY a.devEui"

  const [query1Result, query2Result] = await Promise.all(
      [
        queryParser.getAllRows(QUERY1),
        queryParser.getAllRows(QUERY2),
      ],
  );

  // Map data for complete response

  return query2Result;
}

/**
 * Prepare devices devEui for querry.
 * Get table data
 * @param {string} item - DevEuis for query
 * @param {string} index - DevEuis for query
 * @param {[]} arr - DevEuis for query
 */
function preProcessing(item, index, arr) {
  arr[index] = "devEui='" + item +"'";
}

module.exports = {
  recentlyAddedData,
  recentlyAddedDataFromDevice,
  dailyConsumptionFromDevice,
  weeklyConsumptionFromDevice,
  monthlyConsumptionFromDevice,
  consumptionPeriodFromDevice,
  hourlyConsumptionFromDevice,
  hourlyTempGraph,
  tableData,
  changeTableData,
  fullTableData,
  fullTableDataV2,
};
