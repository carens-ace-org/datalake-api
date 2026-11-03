package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class GetPolicyDetailsRequestProcessor extends MbJavaComputeNode {

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
            // 1. Extract query parameters
            String policyIdStr = getParam(inAssembly, "policyId", "PolicyId", "PolicyID");
            String countryRaw = getParam(inAssembly, "country", "Country", "Country_Name");

            boolean hasPolicyId = (policyIdStr != null && !policyIdStr.trim().isEmpty());
            boolean hasCountry = (countryRaw != null && !countryRaw.trim().isEmpty());

            if (!hasPolicyId || !hasCountry) {
                sendAlternateResponse(alternateMessage, outLocalEnv, 400, false, 
                    "Required query parameters missing: policyId and country must be provided.");
                alternate.propagate(alternateAssembly);
                return;
            }
           
            String country = countryRaw.trim().toUpperCase();

            long policyId;
            try {
                policyId = Long.parseLong(policyIdStr.trim());
            } catch (NumberFormatException nfe) {
                sendAlternateResponse(alternateMessage, outLocalEnv, 400, false, 
                    "Invalid PolicyID format. Must be a numeric integer.");
                alternate.propagate(alternateAssembly);
                return;
            }

            // 2. Obtain JDBC Connection
            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);

            // 3. SQL Query strictly as provided
            String sql = 
                "SELECT fp.\"PolicyID\", " +
                "fp.\"Policy_Status\", " +
                "fp.\"Policy_Active\", " +
                "fp.\"Product_ID\", " +
                "db.\"Product\", " +
                "fp.\"Effective_Date\" " +
                "FROM public.\"Actisure_Fact_Policy\" fp " +
                "JOIN public.\"Actisure_Dim_Product\" db ON db.\"Product_ID\" = fp.\"Product_ID\" " +
                "WHERE fp.\"PolicyID\" = ? " +
                "AND fp.\"Country_Name\" = ?";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setLong(1, policyId);
                ps.setString(2, country); // Passed as uppercase

                try (ResultSet rs = ps.executeQuery()) {
                    MbElement root = outMessage.getRootElement();

                    // HTTP Headers
                    MbElement httpHeader = root.createElementAsLastChild("HTTPRequestHeader");
                    httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

                    // Response Body Root
                    MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
                    jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", true);
                    jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", "Member data retrieved successfully");
                    jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "statusCode", 200);

                    // JSON Data Array
                    MbElement dataArray = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "data", null);
                    dataArray.setSpecificType(MbJSON.ARRAY);

                    boolean hasRecords = false;

                    while (rs.next()) {
                        hasRecords = true;

                        MbElement itemObj = dataArray.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
                        itemObj.setSpecificType(MbJSON.OBJECT);

                        // Read columns from the strict SQL query
                        long currentPolicyId = rs.getLong("PolicyID");
                        String policyStatus = rs.getString("Policy_Status");
                        String product = rs.getString("Product");

                        // Construct Scheme_Name dynamically
                        String schemeName = (product != null) ? product + " Scheme" : "";

                        // Map strictly to expected payload keys with UPPERCASE Country
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "PolicyID", currentPolicyId);
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Policy_Status", (policyStatus != null) ? policyStatus : "");
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Product", (product != null) ? product : "");
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Scheme_Name", schemeName);
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Country", country);
                    }

                    if (hasRecords) {
                        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "responseCode", "SUCCESS");
                        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "status", "00");
                        out.propagate(outAssembly);
                    } else {
                        sendAlternateResponse(alternateMessage, outLocalEnv, 404, false, 
                            "No policy details found for policyId: " + policyIdStr + " and country: " + country);
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