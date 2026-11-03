package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class GetDimCustomersRequestProcessor extends MbJavaComputeNode {

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
            // 1. Extract required query parameters
            String fromDate = getParam(inAssembly, "fromDate", "FromDate");
            String toDate = getParam(inAssembly, "toDate", "ToDate");
            String status = getParam(inAssembly, "status", "Status");

            boolean hasFromDate = (fromDate != null && !fromDate.trim().isEmpty());
            boolean hasToDate = (toDate != null && !toDate.trim().isEmpty());
            boolean hasStatus = (status != null && !status.trim().isEmpty());

            if (!hasFromDate || !hasToDate || !hasStatus) {
                sendAlternateResponse(alternateMessage, outLocalEnv, 400, false, 
                    "Required query parameters missing: fromDate, toDate, and status (NEW or UPDATED) are required.");
                alternate.propagate(alternateAssembly);
                return;
            }

            String cleanStatus = status.trim();
            if (!"NEW".equals(cleanStatus) && !"UPDATED".equals(cleanStatus)) {
                sendAlternateResponse(alternateMessage, outLocalEnv, 400, false, 
                    "Invalid status filter. Status must be case-sensitively 'NEW' or 'UPDATED'.");
                alternate.propagate(alternateAssembly);
                return;
            }

            // 2. Determine target timestamp column based on status
            String dateColumn = "NEW".equals(cleanStatus) ? "\"_date_loaded\"" : "\"_date_changed\"";

            String startTimestampStr = formatStartTimestamp(fromDate.trim());
            String endTimestampStr = formatEndTimestamp(toDate.trim());

            // 3. Obtain JDBC Connection via policy
            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);

            // 4. SQL query
            String sql = 
                "SELECT * " +
                "FROM \"DIM_JHL_CUSTOMER\" " +
                "WHERE " + dateColumn + " >= ?::timestamp AND " + dateColumn + " < ?::timestamp";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, startTimestampStr);
                ps.setString(2, endTimestampStr);

                try (ResultSet rs = ps.executeQuery()) {
                    MbElement root = outMessage.getRootElement();

                    // HTTP Response Headers
                    MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
                    httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

                    // Top-level JSON Array
                    MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
                    jsonRoot.setSpecificType(MbJSON.ARRAY);

                    ResultSetMetaData rsmd = rs.getMetaData();
                    int columnCount = rsmd.getColumnCount();
                    boolean hasRecords = false;

                    while (rs.next()) {
                        hasRecords = true;

                        MbElement itemObj = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
                        itemObj.setSpecificType(MbJSON.OBJECT);

                        for (int i = 1; i <= columnCount; i++) {
                            String colName = rsmd.getColumnLabel(i);
                            Object colValue = rs.getObject(i);

                            if (colValue == null) {
                                // Add JSON null literal
                                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, null);
                            } else if (colValue instanceof Number) {
                                // Keep native numeric types (Integer, Long, Double) for JSON
                                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue);
                            } else if (colValue instanceof Boolean) {
                                // Keep boolean types
                                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue);
                            } else if (colValue instanceof Timestamp) {
                                // Format timestamp to match target string output format (YYYY-MM-DD HH:mm:ss.SSS)
                                SimpleDateFormat tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, tsFormat.format((Timestamp) colValue));
                            } else {
                                // Standard string fallback
                                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue.toString());
                            }
                        }
                    }

                    if (hasRecords) {
                        out.propagate(outAssembly);
                    } else {
                        sendAlternateResponse(alternateMessage, outLocalEnv, 404, false, 
                            "No " + cleanStatus + " customer records found between " + fromDate + " and " + toDate);
                        alternate.propagate(alternateAssembly);
                    }
                }
            }

        } catch (Exception e) {
            sendAlternateResponse(alternateMessage, outLocalEnv, 500, false, 
                "Internal Server Error: " + e.getMessage());
            alternate.propagate(alternateAssembly);
        }
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

    private void sendAlternateResponse(MbMessage altMessage, MbMessage altLocalEnv, int httpStatusCode, boolean success, String message) throws MbException {
        MbElement localEnvRoot = altLocalEnv.getRootElement();
        MbElement dest = localEnvRoot.getFirstElementByPath("Destination");
        if (dest == null) {
            dest = localEnvRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Destination", null);
        }

        MbElement http = dest.getFirstElementByPath("HTTP");
        if (http == null) {
            http = dest.createElementAsLastChild(MbElement.TYPE_NAME, "HTTP", null);
        }

        MbElement replyCode = http.getFirstElementByPath("ReplyStatusCode");
        if (replyCode == null) {
            http.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "ReplyStatusCode", httpStatusCode);
        } else {
            replyCode.setValue(httpStatusCode);
        }

        MbElement root = altMessage.getRootElement();
        MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", String.valueOf(success));
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", message);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "statusCode", httpStatusCode);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "timestamp", getCurrentTimestampIso());
    }

    private String getParam(MbMessageAssembly inAssembly, String... keys) {
        try {
            MbElement root = inAssembly.getLocalEnvironment().getRootElement();

            MbElement restParams = root.getFirstElementByPath("REST/Input/Parameters");
            if (restParams != null) {
                for (String key : keys) {
                    MbElement el = restParams.getFirstElementByPath(key);
                    if (el != null && el.getValueAsString() != null && !el.getValueAsString().trim().isEmpty()) {
                        return el.getValueAsString().trim();
                    }
                }
            }

            MbElement queryParams = root.getFirstElementByPath("HTTP/Input/QueryString");
            if (queryParams != null) {
                for (String key : keys) {
                    MbElement el = queryParams.getFirstElementByPath(key);
                    if (el != null && el.getValueAsString() != null && !el.getValueAsString().trim().isEmpty()) {
                        return el.getValueAsString().trim();
                    }
                }
            }
        } catch (Exception ignored) {}
        return null;
    }

    private String getCurrentTimestampIso() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TimeZone.getTimeZone("Africa/Nairobi"));
        return sdf.format(new Date());
    }
}