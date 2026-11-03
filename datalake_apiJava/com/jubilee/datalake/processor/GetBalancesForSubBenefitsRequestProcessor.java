package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class GetBalancesForSubBenefitsRequestProcessor extends MbJavaComputeNode {

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
            // 1. Extract beneficiaryId and country parameters
            String beneficiaryId = getParam(inAssembly, "beneficiaryId", "BeneficiaryId", "EntityID", "entityId");
            String country = getParam(inAssembly, "country", "Country", "Country_Name");

            boolean hasBeneficiaryId = (beneficiaryId != null && !beneficiaryId.trim().isEmpty());
            boolean hasCountry = (country != null && !country.trim().isEmpty());

            if (!hasBeneficiaryId || !hasCountry) {
                // HTTP 400: Missing query parameters
                sendAlternateResponse(alternateMessage, outLocalEnv, 400, false, "Both beneficiaryId and country parameters are required.");
                alternate.propagate(alternateAssembly);
                return;
            }

            // 2. Database Query
            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);

            String sql = 
                "WITH CTE_BALANCES_SUB_BENEFITS AS (" +
                "    SELECT " +
                "        ROW_NUMBER() OVER (" +
                "            PARTITION BY SubBenefitId " +
                "            ORDER BY EffectiveDate DESC" +
                "        ) AS ROW_NUM, " +
                "        PolicyId, MasterPolicyId, BenefitId, Benefit, SubBenefitId, SubBenefit, BALANCE, EffectiveDate " +
                "    FROM V_ACTISURE_FACT_BALANCES_SUB_BENEFITS " +
                "    WHERE TRIM(BeneficiaryId) = TRIM(?) " +
                "      AND EffectiveDate <= CURRENT_TIMESTAMP " +
                "      AND UPPER(TRIM(Country_Name)) = UPPER(TRIM(?))" +
                ") " +
                "SELECT " +
                "    BenefitId, Benefit, SubBenefitId, SubBenefit, BALANCE " +
                "FROM CTE_BALANCES_SUB_BENEFITS " +
                "WHERE ROW_NUM = 1";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, beneficiaryId.trim());
                ps.setString(2, country.trim());

                try (ResultSet rs = ps.executeQuery()) {

                    MbElement root = outMessage.getRootElement();

                    // Set HTTP Headers
                    MbElement httpHeader = root.createElementAsLastChild("HTTPRequestHeader");
                    httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

                    // -------------------------------------------------------------
                    // Build Raw Unwrapped JSON Array [...]
                    // -------------------------------------------------------------
                    MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
                    jsonRoot.setSpecificType(MbJSON.ARRAY);

                    boolean hasRecords = false;

                    while (rs.next()) {
                        hasRecords = true;

                        MbElement itemObj = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
                        itemObj.setSpecificType(MbJSON.OBJECT);

                        long benefitId = rs.getLong("BenefitId");
                        String benefit = rs.getString("Benefit");
                        long subBenefitId = rs.getLong("SubBenefitId");
                        String subBenefit = rs.getString("SubBenefit");
                        BigDecimal balance = rs.getBigDecimal("BALANCE");

                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "BenefitId", benefitId);
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Benefit", benefit != null ? benefit.trim() : "");
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "SubBenefitId", subBenefitId);
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "SubBenefit", subBenefit != null ? subBenefit.trim() : "");
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Balance", balance != null ? balance : BigDecimal.ZERO);
                    }

                    if (hasRecords) {
                        // HTTP 200 SUCCESS -> Propagate raw array to OUT terminal
                        out.propagate(outAssembly);
                    } else {
                        // HTTP 400: Record not found in DB
                        sendAlternateResponse(alternateMessage, outLocalEnv, 400, false, "No sub-benefit balance records found matching beneficiaryId: " + beneficiaryId + " and country: " + country);
                        alternate.propagate(alternateAssembly);
                    }
                }
            }

        } catch (Exception e) {
            // HTTP 500: Internal Server Error
            sendAlternateResponse(alternateMessage, outLocalEnv, 500, false, "Internal Server Error: " + e.getMessage());
            alternate.propagate(alternateAssembly);
        }
    }

    /**
     * Helper Method: Sets HTTP Status Code in LocalEnvironment and formats JSON error response
     */
    private void sendAlternateResponse(MbMessage altMessage, MbMessage altLocalEnv, int httpStatusCode, boolean success, String message) throws MbException {

        // 1. Set HTTP Reply Status Code in Local Environment
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

        // 2. Set HTTP Response Headers
        MbElement root = altMessage.getRootElement();
        MbElement httpHeader = root.createElementAsLastChild("HTTPRequestHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        // 3. Set JSON Body: { "success": "false", "message": "..." }
        MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", String.valueOf(success));
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", message);
    }

    /**
     * Helper Method: Safely extracts query or REST path parameters from LocalEnvironment
     */
    private String getParam(MbMessageAssembly inAssembly, String... keys) {
        try {
            MbElement root = inAssembly.getLocalEnvironment().getRootElement();

            // 1. Check REST/Input/Parameters
            MbElement restParams = root.getFirstElementByPath("REST/Input/Parameters");
            if (restParams != null) {
                for (String key : keys) {
                    MbElement el = restParams.getFirstElementByPath(key);
                    if (el != null && el.getValueAsString() != null && !el.getValueAsString().trim().isEmpty()) {
                        return el.getValueAsString().trim();
                    }
                }
            }

            // 2. Check HTTP/Input/QueryString
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