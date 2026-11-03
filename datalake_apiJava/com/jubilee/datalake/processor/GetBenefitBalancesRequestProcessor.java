package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class GetBenefitBalancesRequestProcessor extends MbJavaComputeNode {

    private static final String BENEFIT_BALANCES_SQL = """
        WITH CTE_BALANCES_MAIN_BENEFITS AS (
            SELECT 
                ROW_NUMBER() OVER (
                    PARTITION BY BenefitId 
                    ORDER BY EffectiveDate DESC
                ) AS ROW_NUM, 
                PolicyId, 
                MasterPolicyId, 
                BenefitId, 
                Benefit, 
                BALANCE, 
                EffectiveDate 
            FROM "V_ACTISURE_FACT_BALANCES_MAIN_BENEFITS" 
            WHERE TRIM("BeneficiaryId") = TRIM(?) 
              AND "EffectiveDate" <= CURRENT_TIMESTAMP 
              AND UPPER(TRIM("Country_Name")) = UPPER(TRIM(?))
        ) 
        SELECT 
            "BenefitId", 
            "Benefit", 
            "BALANCE" 
        FROM CTE_BALANCES_MAIN_BENEFITS 
        WHERE ROW_NUM = 1
        """;

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
                // HTTP 400: Missing required parameters
                sendAlternateResponse(alternateMessage, outLocalEnv, 400, false, "Both beneficiaryId and country parameters are required.");
                alternate.propagate(alternateAssembly);
                return;
            }

            // 2. Database Query via Policy Pool Connection
            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);

            try (PreparedStatement ps = conn.prepareStatement(BENEFIT_BALANCES_SQL)) {
                ps.setString(1, beneficiaryId.trim());
                ps.setString(2, country.trim());

                try (ResultSet rs = ps.executeQuery()) {
                    MbElement root = outMessage.getRootElement();

                    // Set HTTP Reply Headers
                    MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
                    httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

                    // Build Unwrapped Raw JSON Array [...]
                    MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
                    jsonRoot.setSpecificType(MbJSON.ARRAY);

                    boolean hasRecords = false;

                    while (rs.next()) {
                        hasRecords = true;

                        MbElement itemObj = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
                        itemObj.setSpecificType(MbJSON.OBJECT);

                        long benefitId = rs.getLong("BenefitId");
                        String benefit = rs.getString("Benefit");
                        BigDecimal balance = rs.getBigDecimal("BALANCE");

                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "BenefitId", benefitId);
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Benefit", benefit != null ? benefit.trim() : "");
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Balance", balance != null ? balance : BigDecimal.ZERO);
                    }

                    if (hasRecords) {
                        // HTTP 200 SUCCESS -> Propagate array to OUT terminal
                        out.propagate(outAssembly);
                    } else {
                        // HTTP 404 / 400: No records found
                        sendAlternateResponse(alternateMessage, outLocalEnv, 400, false, 
                            "No benefit balance records found matching beneficiaryId: " + beneficiaryId + " and country: " + country);
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
     * Sets HTTP Status Code in LocalEnvironment and formats JSON error response.
     */
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
    }

    /**
     * Safely extracts query, REST path parameters, or JSON body fields from incoming Assembly.
     */
    private String getParam(MbMessageAssembly inAssembly, String... keys) {
        try {
            // 1. Check Body Payload (JSON/Data/...)
            MbElement msgRoot = inAssembly.getMessage().getRootElement();
            for (String key : keys) {
                MbElement jsonEl = msgRoot.getFirstElementByPath("JSON/Data/" + key);
                if (jsonEl != null && jsonEl.getValueAsString() != null && !jsonEl.getValueAsString().trim().isEmpty()) {
                    return jsonEl.getValueAsString().trim();
                }
            }

            // 2. Check REST & Query String Parameters
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