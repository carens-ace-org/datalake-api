package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class GetCrmMembersRequestProcessor extends MbJavaComputeNode {

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
            String beneficiaryIdStr = getParam(inAssembly, "beneficiaryId", "BeneficiaryId", "BENEFICIARY_ID");
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

            // 2. Obtain JDBC Connection via configured ACE policy
            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);

            // 3. PostgreSQL CTE Query strictly returning the 5 targeted fields
            String sql = 
                "WITH CTE_LATEST_POLICY AS (" +
                "  SELECT ROW_NUMBER() OVER (PARTITION BY \"PolicyId\" ORDER BY \"EffectiveDate\" DESC) AS ROW_NUM, " +
                "         \"PolicyId\", \"EffectiveDate\", \"BeneficiaryId\" " +
                "  FROM public.\"ACTISURE_POLICY_BENEFICIARIES\" " +
                "  WHERE \"BeneficiaryId\" = ? " +
                "    AND \"EffectiveDate\" <= CURRENT_TIMESTAMP " +
                "    AND UPPER(TRIM(\"Country_Name\")) = UPPER(TRIM(?))" +
                "), " +
                "CTE_MEMBER AS (" +
                "  SELECT PB.\"Principal_Id\", " +
                "         PB.\"BeneficiaryName\", " +
                "         PB.\"PrincipalName\", " +
                "         SC.\"Scheme_Name\", " +
                "         PB.\"Country_Name\" " +
                "  FROM CTE_LATEST_POLICY PL " +
                "  JOIN public.\"ACTISURE_POLICY_BENEFICIARIES\" PB ON PL.\"PolicyId\" = PB.\"PolicyId\" " +
                "   AND PL.\"EffectiveDate\" = PB.\"EffectiveDate\" " +
                "   AND PL.\"BeneficiaryId\" = PB.\"BeneficiaryId\" " +
                "   AND PL.ROW_NUM = 1 " +
                "  LEFT JOIN public.\"Actisure_Dim_Scheme\" SC ON PB.\"PolicyHolderId\" = SC.\"Scheme_ID\" " +
                "   AND PB.\"Country_Name\" = SC.\"Country\" " +
                "  WHERE PB.\"EffectiveDate\" <= CURRENT_TIMESTAMP " +
                "    AND UPPER(TRIM(PB.\"Country_Name\")) = UPPER(TRIM(?))" +
                ") " +
                "SELECT \"Principal_Id\", \"BeneficiaryName\", \"PrincipalName\", \"Scheme_Name\", \"Country_Name\" " +
                "FROM CTE_MEMBER";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                // Set CTE filter parameters
                ps.setLong(1, beneficiaryId);
                ps.setString(2, country.trim());
                ps.setString(3, country.trim());

                try (ResultSet rs = ps.executeQuery()) {
                    MbElement root = outMessage.getRootElement();

                    // Set HTTP Response Headers
                    MbElement httpHeader = root.createElementAsLastChild("HTTPRequestHeader");
                    httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

                    // Top-level JSON Array
                    MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
                    jsonRoot.setSpecificType(MbJSON.ARRAY);

                    boolean hasRecords = false;

                    while (rs.next()) {
                        hasRecords = true;

                        MbElement itemObj = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
                        itemObj.setSpecificType(MbJSON.OBJECT);

                        // Strictly map the 5 target response fields
                        long principalId = rs.getLong("Principal_Id");
                        boolean isPrincipalNull = rs.wasNull();
                        String beneficiaryName = rs.getString("BeneficiaryName");
                        String principalName = rs.getString("PrincipalName");
                        String schemeName = rs.getString("Scheme_Name");
                        String countryName = rs.getString("Country_Name");

                        itemObj.createElementAsLastChild(
                            MbElement.TYPE_NAME_VALUE, 
                            "Principal_Id", 
                            isPrincipalNull ? null : principalId
                        );
                        itemObj.createElementAsLastChild(
                            MbElement.TYPE_NAME_VALUE, 
                            "BeneficiaryName", 
                            beneficiaryName != null ? beneficiaryName : ""
                        );
                        itemObj.createElementAsLastChild(
                            MbElement.TYPE_NAME_VALUE, 
                            "PrincipalName", 
                            principalName != null ? principalName : ""
                        );
                        itemObj.createElementAsLastChild(
                            MbElement.TYPE_NAME_VALUE, 
                            "Scheme_Name", 
                            schemeName != null ? schemeName : ""
                        );
                        itemObj.createElementAsLastChild(
                            MbElement.TYPE_NAME_VALUE, 
                            "Country_Name", 
                            countryName != null ? countryName : ""
                        );
                    }

                    if (hasRecords) {
                        out.propagate(outAssembly);
                    } else {
                        sendAlternateResponse(alternateMessage, outLocalEnv, 404, false, 
                            "No member records found for beneficiaryId: " + beneficiaryIdStr + " and country: " + country);
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

    private String getCurrentTimestampIso() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TimeZone.getTimeZone("Africa/Nairobi"));
        return sdf.format(new Date());
    }
}