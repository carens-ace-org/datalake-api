package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class GetCardTypesRequestProcessor extends MbJavaComputeNode {

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
            // 1. Extract required 'country' query parameter
            String country = getParam(inAssembly, "country", "Country", "Country_Name");

            if (country == null || country.trim().isEmpty()) {
                sendAlternateResponse(alternateMessage, outLocalEnv, 400, false, 
                    "Required query parameter missing: country must be provided.");
                alternate.propagate(alternateAssembly);
                return;
            }

            // 2. Obtain JDBC Connection via configured ACE policy
            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);

            // 3. SQL query
            String sql = 
                "SELECT \"CardTypeId\", \"CardTypeName\", \"Country\" " +
                "FROM \"Actisure_Dim_CardTypes\" " +
                "WHERE UPPER(TRIM(\"Country\")) = UPPER(TRIM(?))";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, country.trim());

                try (ResultSet rs = ps.executeQuery()) {
                    MbElement root = outMessage.getRootElement();

                    // HTTP Headers
                    MbElement httpHeader = root.createElementAsLastChild("HTTPRequestHeader");
                    httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

                    // Top-level JSON Array structure
                    MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
                    jsonRoot.setSpecificType(MbJSON.ARRAY);

                    boolean hasRecords = false;

                    while (rs.next()) {
                        hasRecords = true;
                        String cardTypeName = rs.getString("CardTypeName");
                        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Item", 
                            (cardTypeName != null) ? cardTypeName : "");
                    }

                    if (hasRecords) {
                        out.propagate(outAssembly);
                    } else {
                        sendAlternateResponse(alternateMessage, outLocalEnv, 404, false, 
                            "No card types found for country: " + country);
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
        MbElement httpHeader = root.createElementAsLastChild("HTTPRequestHeader");
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