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

public class GetActisureDimSchemeCustomersProcessor extends MbJavaComputeNode {

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
            // 1. Extract Query Parameters
            String fromDate = getParam(inAssembly, "fromDate");
            String toDate = getParam(inAssembly, "toDate");
            String status = getParam(inAssembly, "status");

            // 2. Validate Input Parameters
            if (fromDate == null || fromDate.isEmpty() || 
                toDate == null || toDate.isEmpty() || 
                status == null || status.isEmpty()) {
                sendErrorResponse(alternateMessage, outLocalEnv, 400, "Missing required query parameters: fromDate, toDate, and status are mandatory.");
                alternate.propagate(alternateAssembly);
                return;
            }

            String cleanStatus = status.trim();
            if (!"NEW".equalsIgnoreCase(cleanStatus) && !"UPDATED".equalsIgnoreCase(cleanStatus)) {
                sendErrorResponse(alternateMessage, outLocalEnv, 400, "Invalid status parameter. Must be either 'NEW' or 'UPDATED'.");
                alternate.propagate(alternateAssembly);
                return;
            }

            // 3. Construct Variant SQL Query & Boundaries
            String dateColumn = "NEW".equalsIgnoreCase(cleanStatus) ? "\"_date_loaded\"" : "\"_date_changed\"";
            String startTimestampStr = formatStartTimestamp(fromDate.trim());
            String endTimestampStr = formatEndTimestamp(toDate.trim());

            String sql = "SELECT * FROM \"Actisure_Dim_Scheme\" " +
                         "WHERE " + dateColumn + " >= ?::timestamp AND " + dateColumn + " < ?::timestamp";

            // 4. Database Connection via standard CrmStagingDbConnectionPoolManager policy
            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);

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
                    boolean hasRecords = false;

                    SimpleDateFormat tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

                    // 5. Dynamic ResultSet Mapping with Null Omission
                    while (rs.next()) {
                        hasRecords = true;
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

                    if (hasRecords) {
                        out.propagate(outAssembly);
                    } else {
                        sendErrorResponse(alternateMessage, outLocalEnv, 404, "No " + cleanStatus.toUpperCase() + " scheme records found between " + fromDate + " and " + toDate);
                        alternate.propagate(alternateAssembly);
                    }
                }
            }

        } catch (Exception e) {
            sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: " + e.getMessage());
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

    private String getParam(MbMessageAssembly assembly, String key) {
        try {
            MbElement el = assembly.getLocalEnvironment().getRootElement().getFirstElementByPath("REST/Input/Parameters/" + key);
            if (el != null && el.getValueAsString() != null && !el.getValueAsString().trim().isEmpty()) {
                return el.getValueAsString().trim();
            }

            el = assembly.getLocalEnvironment().getRootElement().getFirstElementByPath("HTTP/Input/QueryString/" + key);
            if (el != null && el.getValueAsString() != null && !el.getValueAsString().trim().isEmpty()) {
                return el.getValueAsString().trim();
            }
        } catch (Exception ignored) {}
        return null;
    }

    private void sendErrorResponse(MbMessage altMsg, MbMessage altEnv, int code, String msg) throws MbException {
        MbElement localEnvRoot = altEnv.getRootElement();
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
            http.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "ReplyStatusCode", code);
        } else {
            replyCode.setValue(code);
        }

        MbElement root = altMsg.getRootElement();
        MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        MbElement json = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        json.setSpecificType(MbJSON.OBJECT);
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", false);
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", msg);
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "statusCode", code);
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "responseCode", "ERROR");
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "status", "99");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TimeZone.getTimeZone("Africa/Nairobi"));
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "timestamp", sdf.format(new Date()));
    }
}