package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class GetDimCustomersRequestProcessor extends MbJavaComputeNode {

    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";
    
    private static final String CUSTOMERS_SQL = 
        "SELECT * " +
        "FROM \"DIM_JHL_CUSTOMER\" " +
        "WHERE %s >= ?::timestamp AND %s < ?::timestamp";

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
            String fromDate = Utils.getParam(inAssembly, "fromDate", "FromDate");
            String toDate = Utils.getParam(inAssembly, "toDate", "ToDate");
            String status = Utils.getParam(inAssembly, "status", "Status");

            if (isEmpty(fromDate) || isEmpty(toDate) || isEmpty(status)) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Required query parameters missing: fromDate, toDate, and status (NEW or UPDATED) are required.");
                alternate.propagate(alternateAssembly);
                return;
            }

            String cleanStatus = status.trim();
            if (!"NEW".equals(cleanStatus) && !"UPDATED".equals(cleanStatus)) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Invalid status filter. Status must be case-sensitively 'NEW' or 'UPDATED'.");
                alternate.propagate(alternateAssembly);
                return;
            }

            String dateColumn = "NEW".equals(cleanStatus) ? "\"_date_loaded\"" : "\"_date_changed\"";
            String startTimestamp = formatStartTimestamp(fromDate.trim());
            String endTimestamp = formatEndTimestamp(toDate.trim());

            boolean hasRecords = executeQueryAndBuildResponse(outMessage, dateColumn, startTimestamp, endTimestamp);

            if (hasRecords) {
                out.propagate(outAssembly);
            } else {
                String errorMsg = "No " + cleanStatus + " customer records found between " + fromDate + " and " + toDate;
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 404, errorMsg);
                alternate.propagate(alternateAssembly);
            }

        } catch (Exception e) {
            System.err.println("Database Error: " + e.getMessage());
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: " + e.getMessage());
            alternate.propagate(alternateAssembly);
        }
    }

    private boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    private String formatStartTimestamp(String inputDate) {
        if (inputDate.contains(" ")) {
            return inputDate;
        }
        return inputDate + " 00:00:00";
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

    private boolean executeQueryAndBuildResponse(MbMessage outMessage, String dateColumn, String startTimestamp, String endTimestamp) throws Exception {
        String sql = String.format(CUSTOMERS_SQL, dateColumn, dateColumn);
        boolean hasRecords = false;

        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, startTimestamp);
            ps.setString(2, endTimestamp);

            try (ResultSet rs = ps.executeQuery()) {
                hasRecords = buildJsonPayload(outMessage, rs);
            }
        }
        return hasRecords;
    }

    private boolean buildJsonPayload(MbMessage outMessage, ResultSet rs) throws Exception {
        MbElement root = outMessage.getRootElement();

        MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        jsonRoot.setSpecificType(MbJSON.ARRAY);

        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        boolean hasRecords = false;
        SimpleDateFormat tsFormat = new SimpleDateFormat(TIMESTAMP_FORMAT);

        while (rs.next()) {
            hasRecords = true;
            MbElement itemObj = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
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
                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, null);
            } else if (colValue instanceof Number || colValue instanceof Boolean) {
                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue);
            } else if (colValue instanceof Timestamp) {
                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, tsFormat.format((Timestamp) colValue));
            } else {
                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue.toString());
            }
        }
    }
}