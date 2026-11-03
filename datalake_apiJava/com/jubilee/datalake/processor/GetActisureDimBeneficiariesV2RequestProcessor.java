package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class GetActisureDimBeneficiariesV2RequestProcessor extends MbJavaComputeNode {

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
            // 1. Extract and parse pagination query parameters
            String pageNumStr = getParam(inAssembly, "pageNumber", "PageNumber");
            String pageSizeStr = getParam(inAssembly, "pageSize", "PageSize");

            int pageNumber = 0;
            int pageSize = 10;

            if (pageNumStr != null && !pageNumStr.trim().isEmpty()) {
                try {
                    int parsedPage = Integer.parseInt(pageNumStr.trim());
                    if (parsedPage >= 0) pageNumber = parsedPage;
                } catch (NumberFormatException ignored) {}
            }

            if (pageSizeStr != null && !pageSizeStr.trim().isEmpty()) {
                try {
                    int parsedSize = Integer.parseInt(pageSizeStr.trim());
                    if (parsedSize > 0) pageSize = parsedSize;
                } catch (NumberFormatException ignored) {}
            }

            int limit = pageSize;
            int offset = pageNumber * pageSize;

            // 2. Obtain JDBC Connection via policy
            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);

            // 3. ISF Query Execution
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

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, limit);
                ps.setInt(2, offset);

                try (ResultSet rs = ps.executeQuery()) {
                    MbElement root = outMessage.getRootElement();

                    // HTTP Response Headers
                    MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
                    httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

                    // Response Root JSON Object
                    MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
                    jsonRoot.setSpecificType(MbJSON.OBJECT);

                    // Response Metadata
                    jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", true);
                    jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", "Data retrieved successfully");
                    jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "statusCode", 200);

                    // Data Array
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

                            // Omit null fields completely from JSON output
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

        } catch (Exception e) {
        	
            
        	System.err.println("Database Error: " + e.getMessage()); 
        	
            sendAlternateResponse(alternateMessage, outLocalEnv, 500, false, "Internal Server Error: There was a problem establishing a connection to the given database URL.");

        	
            alternate.propagate(alternateAssembly);
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
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", success);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", message);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "statusCode", httpStatusCode);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "responseCode", "ERROR");
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "status", "99");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TimeZone.getTimeZone("Africa/Nairobi"));
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "timestamp", sdf.format(new Date()));
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
}