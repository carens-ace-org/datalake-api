package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class GetIsfDimBeneficiariesV2Processor extends MbJavaComputeNode {

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
            // 1. Extract & Parse Pagination Parameters
            String pageNumberStr = getParam(inAssembly, "pageNumber");
            String pageSizeStr = getParam(inAssembly, "pageSize");

            int pageNumber = (pageNumberStr != null && !pageNumberStr.trim().isEmpty()) ? Integer.parseInt(pageNumberStr.trim()) : 0;
            int pageSize = (pageSizeStr != null && !pageSizeStr.trim().isEmpty()) ? Integer.parseInt(pageSizeStr.trim()) : 10;

            if (pageNumber < 0 || pageSize <= 0) {
                sendErrorResponse(alternateMessage, outLocalEnv, 400, "pageNumber must be >= 0 and pageSize must be > 0.");
                alternate.propagate(alternateAssembly);
                return;
            }

            int offset = pageNumber * pageSize;

            // 2. SQL Query 
            String sql = "SELECT * " +
                         "FROM public.\"ISF_Dim_Beneficiary\" b " +
                         "JOIN public.\"ISF_Fact_Policy\" p ON b.\"POLICY_NUMBER\"::text = p.\"POLICY_NUMBER\"::text " +
                         "JOIN public.\"ISF_Dim_Product\" pr ON p.\"PRODUCT_CODE\"::text = pr.\"PRODUCT_CODE\"::text " +
                         "AND p.\"COUNTRY\"::text = pr.\"COUNTRY\"::text " +
                         "WHERE p.\"PAYER_REF_NO\"::text <> p.\"LIFE_ASSURED_REF_NO\"::text " +
                         "AND (DATE(b.\"LOAD_DATE\") = CURRENT_DATE - 1 " +
                         "OR DATE(b.\"UPDATE_DATE\") = CURRENT_DATE - 1) " +
                         "LIMIT ? " +
                         "OFFSET ?";

            // 3. Database Execution via CrmStagingDbConnectionPoolManager policy
            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, pageSize);
                ps.setInt(2, offset);

                try (ResultSet rs = ps.executeQuery()) {
                    // 4. JSON Payload Response Construction
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

                    ResultSetMetaData rsmd = rs.getMetaData();
                    int columnCount = rsmd.getColumnCount();
                    SimpleDateFormat tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

                    while (rs.next()) {
                        MbElement itemObj = dataArray.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
                        itemObj.setSpecificType(MbJSON.OBJECT);

                        for (int i = 1; i <= columnCount; i++) {
                            String colName = rsmd.getColumnLabel(i);
                            Object colValue = rs.getObject(i);

                            // Omit null values completely from JSON output
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

                    jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "responseCode", "SUCCESS");
                    jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "status", "00");

                    out.propagate(outAssembly);
                }
            }

        } catch (NumberFormatException e) {
            sendErrorResponse(alternateMessage, outLocalEnv, 400, "Invalid query parameter format. pageNumber and pageSize must be integers.");
            alternate.propagate(alternateAssembly);
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