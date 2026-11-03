package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class GetIsfFactPolicyCustomersV2Processor extends MbJavaComputeNode {

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
            String productCode = getParam(inAssembly, "productCode");

            String pageStr = getParam(inAssembly, "page");
            String sizeStr = getParam(inAssembly, "size");

            // 2. Validate Mandatory Parameters
            if (fromDate == null || fromDate.trim().isEmpty() || 
                toDate == null || toDate.trim().isEmpty() || 
                status == null || status.trim().isEmpty()) {
                sendErrorResponse(alternateMessage, outLocalEnv, 400, "Missing required query parameters: fromDate, toDate, and status are mandatory.");
                alternate.propagate(alternateAssembly);
                return;
            }

            String upperStatus = status.trim().toUpperCase();
            if (!"NEW".equals(upperStatus) && !"UPDATED".equals(upperStatus)) {
                sendErrorResponse(alternateMessage, outLocalEnv, 400, "Invalid status parameter. Must be either 'NEW' or 'UPDATED'.");
                alternate.propagate(alternateAssembly);
                return;
            }

            // 3. Parse Pagination (Defaults: page = 0, size = 400)
            int page = 0;
            int size = 400;

            try {
                if (pageStr != null && !pageStr.trim().isEmpty()) {
                    page = Math.max(0, Integer.parseInt(pageStr.trim()));
                }
                if (sizeStr != null && !sizeStr.trim().isEmpty()) {
                    size = Math.max(1, Integer.parseInt(sizeStr.trim()));
                }
            } catch (NumberFormatException e) {
                sendErrorResponse(alternateMessage, outLocalEnv, 400, "Invalid page or size parameter. Must be an integer.");
                alternate.propagate(alternateAssembly);
                return;
            }

            int offset = page * size;

            // 4.SQL Query
            String targetCol = "NEW".equals(upperStatus) ? "m.\"LOAD_DATE\"" : "m.\"UPDATE_DATE\"";

            StringBuilder sql = new StringBuilder();
            sql.append("SELECT p.\"PRODUCT_NAME\", m.* ");
            sql.append("FROM public.\"ISF_Fact_Policy\" m ");
            sql.append("JOIN public.\"ISF_Dim_Product\" p ON m.\"PRODUCT_CODE\" = p.\"PRODUCT_CODE\" ");
            sql.append("WHERE ").append(targetCol).append("::date >= ?::date ");
            sql.append("  AND ").append(targetCol).append("::date < ?::date ");
            sql.append("  AND (? IS NULL OR p.\"PRODUCT_CODE\" = ?) ");
            sql.append("ORDER BY m.\"LIFE_ASSURED_REF_NO\" ");
            sql.append("LIMIT ? OFFSET ?");

            // 5. Database Execution via CrmStagingDbConnectionPoolManager policy
            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);

            try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                int paramIndex = 1;
                ps.setString(paramIndex++, fromDate.trim());
                ps.setString(paramIndex++, toDate.trim());

                if (productCode != null && !productCode.trim().isEmpty()) {
                    ps.setString(paramIndex++, productCode.trim());
                    ps.setString(paramIndex++, productCode.trim());
                } else {
                    ps.setNull(paramIndex++, Types.VARCHAR);
                    ps.setNull(paramIndex++, Types.VARCHAR);
                }

                ps.setInt(paramIndex++, size);
                ps.setInt(paramIndex++, offset);

                try (ResultSet rs = ps.executeQuery()) {
                    // 6. Direct Unwrapped JSON Array Payload Response Construction
                    MbElement root = outMessage.getRootElement();

                    MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
                    httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

                    MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
                    jsonRoot.setSpecificType(MbJSON.ARRAY);

                    ResultSetMetaData rsmd = rs.getMetaData();
                    int columnCount = rsmd.getColumnCount();
                    SimpleDateFormat tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

                    while (rs.next()) {
                        MbElement itemObj = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
                        itemObj.setSpecificType(MbJSON.OBJECT);

                        for (int i = 1; i <= columnCount; i++) {
                            String colName = rsmd.getColumnLabel(i);
                            Object colValue = rs.getObject(i);

                            // Omit null values completely from JSON payload output
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

                    out.propagate(outAssembly);
                }
            }

        } catch (Exception e) {
            sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: " + e.getMessage());
            alternate.propagate(alternateAssembly);
        }
    }

    private String getParam(MbMessageAssembly assembly, String key) {
        try {
            MbElement root = assembly.getLocalEnvironment().getRootElement();
            MbElement el = root.getFirstElementByPath("REST/Input/Parameters/" + key);
            if (el != null && el.getValueAsString() != null) return el.getValueAsString().trim();

            el = root.getFirstElementByPath("HTTP/Input/QueryString/" + key);
            if (el != null && el.getValueAsString() != null) return el.getValueAsString().trim();
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