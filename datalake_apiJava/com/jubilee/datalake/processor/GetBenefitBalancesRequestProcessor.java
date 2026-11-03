package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class GetBenefitBalancesRequestProcessor extends MbJavaComputeNode {

    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";
    private static final String BENEFIT_BALANCES_SQL = 
        "WITH CTE_BALANCES_MAIN_BENEFITS AS (" +
        "    SELECT " +
        "        ROW_NUMBER() OVER (" +
        "            PARTITION BY BenefitId " +
        "            ORDER BY EffectiveDate DESC" +
        "        ) AS ROW_NUM, " +
        "        PolicyId, " +
        "        MasterPolicyId, " +
        "        BenefitId, " +
        "        Benefit, " +
        "        BALANCE, " +
        "        EffectiveDate " +
        "    FROM \"V_ACTISURE_FACT_BALANCES_MAIN_BENEFITS\" " +
        "    WHERE TRIM(\"BeneficiaryId\") = TRIM(?) " +
        "      AND \"EffectiveDate\" <= CURRENT_TIMESTAMP " +
        "      AND UPPER(TRIM(\"Country_Name\")) = UPPER(TRIM(?))" +
        ") " +
        "SELECT " +
        "    \"BenefitId\", " +
        "    \"Benefit\", " +
        "    \"BALANCE\" " +
        "FROM CTE_BALANCES_MAIN_BENEFITS " +
        "WHERE ROW_NUM = 1";

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
            String beneficiaryId = Utils.getParam(inAssembly, "beneficiaryId", "BeneficiaryId", "EntityID", "entityId");
            String country = Utils.getParam(inAssembly, "country", "Country", "Country_Name");

            if (beneficiaryId == null || beneficiaryId.trim().isEmpty() || country == null || country.trim().isEmpty()) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Both beneficiaryId and country parameters are required.");
                alternate.propagate(alternateAssembly);
                return;
            }

            boolean recordsFound = executeQueryAndBuildResponse(outMessage, beneficiaryId.trim(), country.trim());

            if (recordsFound) {
                out.propagate(outAssembly);
            } else {
                String errorMsg = String.format("No benefit balance records found matching beneficiaryId: %s and country: %s", beneficiaryId, country);
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, errorMsg);
                alternate.propagate(alternateAssembly);
            }

        } catch (Exception e) {
            System.err.println("Database Error: " + e.getMessage());
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: There was a problem establishing a connection to the given database URL.");
            alternate.propagate(alternateAssembly);
        }
    }

    private boolean executeQueryAndBuildResponse(MbMessage outMessage, String beneficiaryId, String country) throws Exception {
        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(BENEFIT_BALANCES_SQL)) {

            ps.setString(1, beneficiaryId);
            ps.setString(2, country);

            try (ResultSet rs = ps.executeQuery()) {
                return buildJsonPayload(outMessage, rs);
            }
        }
    }

    private boolean buildJsonPayload(MbMessage outMessage, ResultSet rs) throws Exception {
        MbElement root = outMessage.getRootElement();

        MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

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

        return hasRecords;
    }
}