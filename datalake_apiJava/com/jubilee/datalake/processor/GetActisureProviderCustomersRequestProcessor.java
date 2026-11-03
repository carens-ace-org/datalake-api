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

public class GetActisureProviderCustomersRequestProcessor extends MbJavaComputeNode {

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
                    "Required query parameters missing: fromDate, toDate, and status (NEW or UPDATED) must be provided.");
                alternate.propagate(alternateAssembly);
                return;
            }

            String cleanStatus = status.trim();
            if (!"NEW".equalsIgnoreCase(cleanStatus) && !"UPDATED".equalsIgnoreCase(cleanStatus)) {
                sendAlternateResponse(alternateMessage, outLocalEnv, 400, false, 
                    "Invalid status parameter. Must be either 'NEW' or 'UPDATED'.");
                alternate.propagate(alternateAssembly);
                return;
            }

            // 2. Select target date column & construct timestamp boundaries
            String dateColumn = "NEW".equalsIgnoreCase(cleanStatus) ? "\"_date_loaded\"" : "\"_date_changed\"";
            String startTimestampStr = formatStartTimestamp(fromDate.trim());
            String endTimestampStr = formatEndTimestamp(toDate.trim());

            // 3. Obtain JDBC Connection via standard policy
            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);

            // 4. SQL Query Execution with explicit Postgres casting
            String sql = "SELECT * FROM \"Actisure_Dim_Provider\" " +
                         "WHERE " + dateColumn + " >= ?::timestamp AND " + dateColumn + " < ?::timestamp";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, startTimestampStr);
                ps.setString(2, endTimestampStr);

                try (ResultSet rs = ps.executeQuery()) {
                    MbElement root = outMessage.getRootElement();

                    // Set HTTP Reply Header
                    MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
                    httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

                    // Set JSON Root Array
                    MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
                    jsonRoot.setSpecificType(MbJSON.ARRAY);

                    ResultSetMetaData rsmd = rs.getMetaData();
                    int columnCount = rsmd.getColumnCount();

                    SimpleDateFormat tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

                    // 5. Dynamic ResultSet Mapping with Null Omission
                    while (rs.next()) {
                        MbElement itemObj = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
                        itemObj.setSpecificType(MbJSON.OBJECT);

                        for (int i = 1; i <= columnCount; i++) {
                            String colName = rsmd.getColumnLabel(i);
                            Object colValue = rs.getObject(i);

                            // Omit database null fields completely
                            if (colValue == null) {
                                continue;
                            }

                            if (colValue instanceof Number) {
                                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue);
                            } else if (colValue instanceof Boolean) {
                                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue);
                            } else if (colValue instanceof Timestamp) {
                                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, tsFormat.format((Timestamp) colValue));
                            } else {
                                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue.toString());
                            }
                        }
                    }

                    // Always propagate 200 OK with array payload (populated or empty [])
                    out.propagate(outAssembly);
                }
            }

        } catch (Exception e) {
            sendAlternateResponse(alternateMessage, outLocalEnv, 500, false, 
                "Internal Server Error: " + e.getMessage());
            alternate.propagate(alternateAssembly);
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
        jsonRoot.setSpecificType(MbJSON.OBJECT);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", String.valueOf(success));
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", message);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "statusCode", httpStatusCode);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "responseCode", "ERROR");
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "status", "99");
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