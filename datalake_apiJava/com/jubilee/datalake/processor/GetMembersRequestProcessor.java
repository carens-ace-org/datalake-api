package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class GetMembersRequestProcessor extends MbJavaComputeNode {

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
            String beneficiaryIdStr = getParam(inAssembly, "beneficiaryId", "BeneficiaryId", "entityId", "EntityID");
            String country = getParam(inAssembly, "country", "Country", "Country_Name");

            boolean hasBeneficiaryId = (beneficiaryIdStr != null && !beneficiaryIdStr.trim().isEmpty());
            boolean hasCountry = (country != null && !country.trim().isEmpty());

            if (!hasBeneficiaryId || !hasCountry) {
                sendAlternateResponse(alternateMessage, outLocalEnv, 400, false, 
                    "Required query parameters missing: beneficiaryId and country must be provided.");
                alternate.propagate(alternateAssembly);
                return;
            }

            long beneficiaryId;
            try {
                beneficiaryId = Long.parseLong(beneficiaryIdStr.trim());
            } catch (NumberFormatException nfe) {
                sendAlternateResponse(alternateMessage, outLocalEnv, 400, false, 
                    "Invalid beneficiaryId format. Must be a numeric value.");
                alternate.propagate(alternateAssembly);
                return;
            }

            // 2. Database Connection using configured JDBC policy
            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);

           
            String sql = 
                "WITH cte_principal AS (" +
                "    SELECT \"Principal_Id\", MAX(\"EffectiveDate\") AS \"Max_EffectiveDate\" " +
                "    FROM public.\"ACTISURE_POLICY_BENEFICIARIES\" " +
                "    WHERE \"BeneficiaryId\" = ? " +
                "      AND \"EffectiveDate\" <= NOW() " +
                "      AND \"Country_Name\" = ? " +
                "    GROUP BY \"Principal_Id\" " +
                "), " +
                "cte_members AS (" +
                "    SELECT pb.*, sc.\"Scheme_Name\" " +
                "    FROM cte_principal pl " +
                "    JOIN public.\"ACTISURE_POLICY_BENEFICIARIES\" pb ON pl.\"Principal_Id\" = pb.\"Principal_Id\" " +
                "     AND pl.\"Max_EffectiveDate\" = pb.\"EffectiveDate\" " +
                "    LEFT JOIN public.\"Actisure_Dim_Scheme\" sc ON pb.\"PolicyHolderId\" = sc.\"Scheme_ID\" " +
                "     AND pb.\"Country\" = sc.\"Country\" " +
                "    WHERE pb.\"EffectiveDate\" <= NOW() " +
                "      AND pb.\"Country_Name\" = ? " +
                ") " +
                "SELECT * FROM cte_members";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setLong(1, beneficiaryId);
                ps.setString(2, country.trim());
                ps.setString(3, country.trim());

                try (ResultSet rs = ps.executeQuery()) {

                    MbElement root = outMessage.getRootElement();

                    // Set HTTP Response Headers
                    MbElement httpHeader = root.createElementAsLastChild("HTTPRequestHeader");
                    httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

                    // Create JSON Root Structure
                    MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
                    
                    jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", true);
                    jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", "Member data retrieved successfully");
                    jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "statusCode", 200);

                    // Create JSON Array for Data
                    MbElement dataArray = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "data", null);
                    dataArray.setSpecificType(MbJSON.ARRAY);

                    boolean hasRecords = false;

                    while (rs.next()) {
                        hasRecords = true;

                        MbElement itemObj = dataArray.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
                        itemObj.setSpecificType(MbJSON.OBJECT);

                        // Safely retrieve columns with fallbacks
                        String smartNo = getColumnString(rs, "SMART_NO", String.valueOf(beneficiaryId));
                        long principalId = rs.getLong("Principal_Id");
                        String beneficiaryName = getColumnString(rs, "BeneficiaryName", "");
                        String principalName = getColumnString(rs, "PrincipalName", beneficiaryName);
                        String idNumber = getColumnString(rs, "ID_Number", "");
                        String schemeName = getColumnString(rs, "Scheme_Name", "");
                        String countryName = getColumnString(rs, "Country_Name", country.trim());

                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "SMART_NO", smartNo);
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Principal_Id", principalId);
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "BeneficiaryName", beneficiaryName);
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "PrincipalName", principalName);
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "ID_Number", idNumber);
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Scheme_Name", schemeName);
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Country_Name", countryName);
                    }

                    if (hasRecords) {
                        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "responseCode", "SUCCESS");
                        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "status", "00");
                        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "timestamp", getCurrentTimestampIso());
                        out.propagate(outAssembly);
                    } else {
                        sendAlternateResponse(alternateMessage, outLocalEnv, 404, false, 
                            "No member records found matching beneficiaryId: " + beneficiaryIdStr + " and country: " + country);
                        alternate.propagate(alternateAssembly);
                    }
                }
            }

        } catch (Exception e) {
            sendAlternateResponse(alternateMessage, outLocalEnv, 500, false, "Internal Server Error: " + e.getMessage());
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
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", success);
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

    private String getColumnString(ResultSet rs, String columnName, String defaultValue) {
        try {
            String val = rs.getString(columnName);
            return (val != null && !val.trim().isEmpty()) ? val.trim() : defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private String getCurrentTimestampIso() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TimeZone.getTimeZone("Africa/Nairobi"));
        return sdf.format(new Date());
    }
}