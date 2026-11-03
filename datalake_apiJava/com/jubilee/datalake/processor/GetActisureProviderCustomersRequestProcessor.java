package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.model.QueryParams;
import com.jubilee.datalake.utils.Utils;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class GetActisureProviderCustomersRequestProcessor extends MbJavaComputeNode {

    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.S";
    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";

    @Override
    public void evaluate(MbMessageAssembly inAssembly) throws MbException {
        MbOutputTerminal out = getOutputTerminal("out");
        MbOutputTerminal alternate = getOutputTerminal("alternate");

        MbMessage outMessage = new MbMessage();
        MbMessage alternateMessage = new MbMessage();
        MbMessage outLocalEnv = new MbMessage(inAssembly.getLocalEnvironment());

        MbMessageAssembly outAssembly = new MbMessageAssembly(inAssembly, outLocalEnv, inAssembly.getExceptionList(), outMessage);
        MbMessageAssembly alternateAssembly = new MbMessageAssembly(inAssembly, outLocalEnv, inAssembly.getExceptionList(), alternateMessage);

        try {
            QueryParams params = Utils.extractAndValidateParams(inAssembly);
            if (params.hasError()) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, params.getErrorCode(), params.getErrorMessage());
                alternate.propagate(alternateAssembly);
                return;
            }

            boolean recordsFound = executeQueryAndBuildResponse(outMessage, params);

            if (recordsFound) {
                out.propagate(outAssembly);
            } else {
                String errorMsg = String.format("No %s provider records found between %s and %s", 
                        params.status.toUpperCase(), params.fromDate, params.toDate);
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 404, errorMsg);
                alternate.propagate(alternateAssembly);
            }

        } catch (Exception e) {
            System.err.println("Database Error: " + e.getMessage());
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: There was a problem establishing a connection to the given database URL.");
            alternate.propagate(alternateAssembly);
        }
    }

    private boolean executeQueryAndBuildResponse(MbMessage outMessage, QueryParams params) throws Exception {
        String dateColumn = "NEW".equals(params.status) ? "\"_date_loaded\"" : "\"_date_changed\"";
        String startTimestampStr = formatStartTimestamp(params.fromDate);
        String endTimestampStr = formatEndTimestamp(params.toDate);

        String sql = "SELECT * FROM \"Actisure_Dim_Provider\" " +
                     "WHERE " + dateColumn + " >= TIMESTAMP '" + startTimestampStr + "' " +
                     "AND " + dateColumn + " < TIMESTAMP '" + endTimestampStr + "'";

        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            return buildJsonPayload(outMessage, rs);
        }
    }

    private boolean buildJsonPayload(MbMessage outMessage, ResultSet rs) throws Exception {
        MbElement root = outMessage.getRootElement();

        MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        jsonRoot.setSpecificType(MbJSON.OBJECT);

        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", true);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", "Data retrieved successfully");
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "statusCode", 200);

        MbElement dataArray = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "data", null);
        dataArray.setSpecificType(MbJSON.ARRAY);

        return populateResultSetRows(rs, dataArray);
    }

    private boolean populateResultSetRows(ResultSet rs, MbElement dataArray) throws Exception {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        SimpleDateFormat tsFormat = new SimpleDateFormat(TIMESTAMP_FORMAT);
        boolean hasRecords = false;

        while (rs.next()) {
            hasRecords = true;
            MbElement itemObj = dataArray.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
            itemObj.setSpecificType(MbJSON.OBJECT);
            mapRowColumns(rs, rsmd, columnCount, itemObj, tsFormat);
        }

        return hasRecords;
    }

    private void mapRowColumns(ResultSet rs, ResultSetMetaData rsmd, int columnCount, MbElement itemObj, SimpleDateFormat tsFormat) throws Exception {
        for (int i = 1; i <= columnCount; i++) {
            String colName = rsmd.getColumnLabel(i);
            Object colValue = rs.getObject(i);

            if (colValue == null) {
                continue;
            }

            if (colValue instanceof Number || colValue instanceof Boolean) {
                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue);
            } else if (colValue instanceof Timestamp) {
                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, tsFormat.format((Timestamp) colValue));
            } else {
                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue.toString());
            }
        }
    }

    private String formatStartTimestamp(String inputDate) {
        return inputDate.contains(" ") ? inputDate : inputDate + " 00:00:00";
    }

    private String formatEndTimestamp(String inputDate) {
        if (inputDate.contains(" ")) {
            return inputDate;
        }
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date d = sdf.parse(inputDate);
            Calendar cal = Calendar.getInstance();
            cal.setTime(d);
            cal.add(Calendar.DAY_OF_MONTH, 1);
            return sdf.format(cal.getTime()) + " 00:00:00";
        } catch (Exception e) {
            return inputDate + " 23:59:59";
        }
    }
}