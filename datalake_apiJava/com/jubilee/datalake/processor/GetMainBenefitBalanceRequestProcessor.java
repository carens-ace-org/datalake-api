package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class GetMainBenefitBalanceRequestProcessor extends MbJavaComputeNode {

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

            // 3. SQL Query
            String sql = 
                "WITH CTE_POLICY_BENEFICIARY AS (" +
                "    SELECT ROW_NUMBER() OVER (PARTITION BY \"BeneficiaryId\" ORDER BY \"EffectiveDate\" DESC) AS \"ROW_NUM\", " +
                "           \"PolicyId\", \"EffectiveDate\", \"BeneficiaryId\", \"Principal_Id\", \"Country_Name\", \"Country\" " +
                "    FROM public.\"ACTISURE_POLICY_BENEFICIARIES\" " +
                "    WHERE \"BeneficiaryId\" = ? " +
                "      AND \"EffectiveDate\" <= CURRENT_DATE " +
                "      AND UPPER(TRIM(\"Country_Name\")) = UPPER(TRIM(?)) " +
                "), " +
                "CTE_LATEST_POLICY_BENEFICIARY AS (" +
                "    SELECT \"PolicyId\", \"BeneficiaryId\", \"Principal_Id\", \"EffectiveDate\", \"Country_Name\", \"Country\" " +
                "    FROM CTE_POLICY_BENEFICIARY " +
                "    WHERE \"ROW_NUM\" = 1 " +
                "), " +
                "CTE_POLICY_BENEFICIARY_BENEFITS_DETAILED AS (" +
                "    SELECT BN.\"PolicyId\", BN.\"ParentPolicyId\", BN.\"Category\" AS \"EnterpriseCategory\", BN.\"EffectiveDate\", " +
                "           PB.\"BeneficiaryId\", PB.\"BeneficiaryName\", PB.\"Relationship\", PB.\"ParentId\", PB.\"Principal_Id\", PB.\"PolicyHolderId\", " +
                "           BN.\"BenefitId\", BN.\"Benefit\", BN.\"BenefitAllocatedAmount\", BN.\"SubBenefitId\", BN.\"SubBenefit\", BN.\"SubBenefitAllocatedAmount\", " +
                "           BN.\"BenefitDetailId\", BN.\"BenefitDetail\", BN.\"BenefitDetailAllocatedAmount\", " +
                "           BN.\"SHARED_BENEFIT\", BN.\"SHARED_SUB_BENEFIT\", BN.\"SHARED_BDL_BENEFIT\", " +
                "           BN.\"CategoryName\", BN.\"WaitingPeriodWaived\", BN.\"ProductCategory\", BN.\"PolicyStatus\", BN.\"PolicyActive\", " +
                "           BN.\"Country\", BN.\"Country_Name\" " +
                "    FROM public.\"ACTISURE_POLICY_BENEFITS\" BN " +
                "    JOIN public.\"ACTISURE_POLICY_BENEFICIARIES\" PB ON BN.\"PolicyId\" = PB.\"PolicyId\" " +
                "     AND BN.\"EffectiveDate\" = PB.\"EffectiveDate\" " +
                "     AND COALESCE(BN.\"Category\", -1) = COALESCE(PB.\"DivisionId\", -1) " +
                "     AND BN.\"Country\" = PB.\"Country\" " +
                "    JOIN CTE_LATEST_POLICY_BENEFICIARY LP ON PB.\"PolicyId\" = LP.\"PolicyId\" " +
                "     AND PB.\"EffectiveDate\" = LP.\"EffectiveDate\" " +
                "     AND PB.\"BeneficiaryId\" = LP.\"BeneficiaryId\" " +
                "     AND PB.\"Country\" = LP.\"Country\" " +
                "), " +
                "CTE_PAID_CLAIMS_DETAILED AS (" +
                "    SELECT PC.\"PolicyId\", PC.\"EffectiveDate\", PC.\"InvoiceBenefitID\" AS \"BenefitId\", PC.\"InvoiceBenefit\" AS \"Benefit\", " +
                "           PC.\"InvoiceSubBenefitID\" AS \"SubBenefitId\", PC.\"InvoiceLineDetailID\" AS \"BenefitDetailId\", " +
                "           PC.\"BeneficiaryId\", PB.\"Principal_Id\", PB.\"PolicyHolderId\", " +
                "           PC.\"SHARED_BENEFIT\", PC.\"SHARED_SUB_BENEFIT\", PC.\"SHARED_BDL_BENEFIT\", PC.\"ClaimAmount\", PC.\"Country\" " +
                "    FROM public.\"ACTISURE_PAID_CLAIMS_TEST_INCREMENTAL\" PC " +
                "    JOIN public.\"ACTISURE_POLICY_BENEFICIARIES\" PB ON PC.\"PolicyId\" = PB.\"PolicyId\" " +
                "     AND PC.\"EffectiveDate\" = PB.\"EffectiveDate\" " +
                "     AND PC.\"BeneficiaryId\" = PB.\"BeneficiaryId\" " +
                "     AND PC.\"Country\" = PB.\"Country\" " +
                "    JOIN CTE_LATEST_POLICY_BENEFICIARY LP ON PB.\"PolicyId\" = LP.\"PolicyId\" " +
                "     AND PB.\"EffectiveDate\" = LP.\"EffectiveDate\" " +
                "     AND PB.\"Principal_Id\" = LP.\"Principal_Id\" " +
                "     AND PB.\"Country\" = LP.\"Country\" " +
                "    WHERE PC._is_deleted != 1 " +
                "), " +
                "CTE_PAID_CLAIMS_SUMMARY AS (" +
                "    SELECT CL.\"PolicyId\", CL.\"EffectiveDate\", CL.\"BenefitId\", CL.\"Benefit\", CL.\"BeneficiaryId\", " +
                "           CL.\"Principal_Id\", CL.\"PolicyHolderId\", CL.\"SHARED_BENEFIT\", SUM(CL.\"ClaimAmount\") AS \"ClaimAmount\", CL.\"Country\" " +
                "    FROM CTE_PAID_CLAIMS_DETAILED CL " +
                "    GROUP BY CL.\"PolicyId\", CL.\"EffectiveDate\", CL.\"BenefitId\", CL.\"Benefit\", CL.\"BeneficiaryId\", " +
                "             CL.\"Principal_Id\", CL.\"PolicyHolderId\", CL.\"SHARED_BENEFIT\", CL.\"Country\" " +
                "), " +
                "CTE_OUTSTANDING_PREAUTHS_SUMMARY AS (" +
                "    SELECT OP.\"PolicyId\", OP.\"EffectiveDate\", OP.\"BeneficiaryId\", OP.\"BenefitId\", " +
                "           SUM(OP.\"OS_PREAUTH_AMOUNT\") AS \"OS_PREAUTH_AMOUNT\", OP.\"Country\" " +
                "    FROM public.\"ACTISURE_OUTSTANDING_PREAUTHS\" OP " +
                "    JOIN CTE_LATEST_POLICY_BENEFICIARY LP ON OP.\"PolicyId\" = LP.\"PolicyId\" " +
                "     AND OP.\"EffectiveDate\" = LP.\"EffectiveDate\" " +
                "     AND OP.\"BeneficiaryId\" = LP.\"BeneficiaryId\" " +
                "     AND OP.\"Country\" = LP.\"Country\" " +
                "    GROUP BY OP.\"PolicyId\", OP.\"EffectiveDate\", OP.\"BeneficiaryId\", OP.\"BenefitId\", OP.\"Country\" " +
                "), " +
                "CTE_PAID_CLAIMS_NON_SHARED_BENEFITS_SUMMARY AS (" +
                "    SELECT PC.\"PolicyId\", PC.\"EffectiveDate\", PC.\"BeneficiaryId\", PC.\"BenefitId\", PC.\"Benefit\", " +
                "           PC.\"Principal_Id\", PC.\"PolicyHolderId\", PC.\"SHARED_BENEFIT\", PC.\"ClaimAmount\", " +
                "           COALESCE(OP.\"OS_PREAUTH_AMOUNT\", 0) AS \"OS_PREAUTH_AMOUNT\", PC.\"Country\" " +
                "    FROM CTE_PAID_CLAIMS_SUMMARY PC " +
                "    LEFT JOIN CTE_OUTSTANDING_PREAUTHS_SUMMARY OP ON PC.\"PolicyId\" = OP.\"PolicyId\" " +
                "     AND PC.\"EffectiveDate\" = OP.\"EffectiveDate\" " +
                "     AND PC.\"BeneficiaryId\" = OP.\"BeneficiaryId\" " +
                "     AND PC.\"BenefitId\" = OP.\"BenefitId\" " +
                "     AND PC.\"Country\" = OP.\"Country\" " +
                "    WHERE PC.\"SHARED_BENEFIT\" = 'N' " +
                "), " +
                "CTE_PAID_CLAIMS_SHARED_BENEFITS_SUMMARY AS (" +
                "    SELECT PC.\"PolicyId\", PC.\"EffectiveDate\", PC.\"BenefitId\", PC.\"Benefit\", PC.\"Principal_Id\", " +
                "           PC.\"PolicyHolderId\", PC.\"SHARED_BENEFIT\", SUM(PC.\"ClaimAmount\") AS \"ClaimAmount\", " +
                "           SUM(COALESCE(OP.\"OS_PREAUTH_AMOUNT\", 0)) AS \"OS_PREAUTH_AMOUNT\", PC.\"Country\" " +
                "    FROM CTE_PAID_CLAIMS_SUMMARY PC " +
                "    LEFT JOIN CTE_OUTSTANDING_PREAUTHS_SUMMARY OP ON PC.\"PolicyId\" = OP.\"PolicyId\" " +
                "     AND PC.\"EffectiveDate\" = OP.\"EffectiveDate\" " +
                "     AND PC.\"BeneficiaryId\" = OP.\"BeneficiaryId\" " +
                "     AND PC.\"BenefitId\" = OP.\"BenefitId\" " +
                "     AND PC.\"Country\" = OP.\"Country\" " +
                "    WHERE PC.\"SHARED_BENEFIT\" = 'Y' " +
                "    GROUP BY PC.\"PolicyId\", PC.\"EffectiveDate\", PC.\"BenefitId\", PC.\"Benefit\", PC.\"Principal_Id\", " +
                "             PC.\"PolicyHolderId\", PC.\"SHARED_BENEFIT\", PC.\"Country\" " +
                "), " +
                "CTE_POLICY_BENEFICIARY_BENEFITS AS (" +
                "    SELECT \"PolicyId\", \"ParentPolicyId\", \"EffectiveDate\", \"BeneficiaryId\", \"BeneficiaryName\", " +
                "           \"Relationship\", \"ParentId\", \"Principal_Id\", \"PolicyHolderId\", \"BenefitId\", \"Benefit\", " +
                "           \"BenefitAllocatedAmount\", \"SHARED_BENEFIT\", \"CategoryName\", \"WaitingPeriodWaived\", " +
                "           \"ProductCategory\", \"PolicyStatus\", \"PolicyActive\", \"Country\", \"Country_Name\" " +
                "    FROM CTE_POLICY_BENEFICIARY_BENEFITS_DETAILED " +
                "    GROUP BY \"PolicyId\", \"ParentPolicyId\", \"EffectiveDate\", \"BeneficiaryId\", \"BeneficiaryName\", " +
                "             \"Relationship\", \"ParentId\", \"Principal_Id\", \"PolicyHolderId\", \"BenefitId\", \"Benefit\", " +
                "             \"BenefitAllocatedAmount\", \"SHARED_BENEFIT\", \"CategoryName\", \"WaitingPeriodWaived\", " +
                "             \"ProductCategory\", \"PolicyStatus\", \"PolicyActive\", \"Country\", \"Country_Name\" " +
                "), " +
                "CTE_BALANCES_BENEFICIARY_NON_SHARED_BENEFITS AS (" +
                "    SELECT PBR.\"PolicyId\", PBR.\"EffectiveDate\", PBR.\"ParentPolicyId\" AS \"MasterPolicyId\", " +
                "           PBR.\"BeneficiaryId\", PBR.\"BeneficiaryName\", PBR.\"Relationship\", PBR.\"ParentId\", " +
                "           PBR.\"Principal_Id\", PBR.\"PolicyHolderId\", PBR.\"BenefitId\", PBR.\"Benefit\", " +
                "           PBR.\"BenefitAllocatedAmount\", PBR.\"SHARED_BENEFIT\", PBR.\"CategoryName\", " +
                "           PBR.\"WaitingPeriodWaived\", PBR.\"ProductCategory\", PBR.\"PolicyStatus\", PBR.\"PolicyActive\", " +
                "           COALESCE(PC.\"ClaimAmount\", 0) AS \"ClaimAmount\", " +
                "           (COALESCE(PBR.\"BenefitAllocatedAmount\", 0) - COALESCE(PC.\"ClaimAmount\", 0) - COALESCE(PC.\"OS_PREAUTH_AMOUNT\", 0)) AS \"BALANCE\", " +
                "           COALESCE(PC.\"OS_PREAUTH_AMOUNT\", 0) AS \"OS_PREAUTH_AMOUNT\", PBR.\"Country\", PBR.\"Country_Name\" " +
                "    FROM CTE_POLICY_BENEFICIARY_BENEFITS PBR " +
                "    LEFT JOIN CTE_PAID_CLAIMS_NON_SHARED_BENEFITS_SUMMARY PC ON PBR.\"PolicyId\" = PC.\"PolicyId\" " +
                "     AND PBR.\"EffectiveDate\" = PC.\"EffectiveDate\" " +
                "     AND PBR.\"BenefitId\" = PC.\"BenefitId\" " +
                "     AND PBR.\"BeneficiaryId\" = PC.\"BeneficiaryId\" " +
                "     AND PBR.\"Principal_Id\" = PC.\"Principal_Id\" " +
                "     AND PBR.\"Country\" = PC.\"Country\" " +
                "    WHERE PBR.\"SHARED_BENEFIT\" = 'N' " +
                "), " +
                "CTE_BALANCES_BENEFICIARY_SHARED_BENEFITS AS (" +
                "    SELECT PBR.\"PolicyId\", PBR.\"EffectiveDate\", PBR.\"ParentPolicyId\" AS \"MasterPolicyId\", " +
                "           PBR.\"BeneficiaryId\", PBR.\"BeneficiaryName\", PBR.\"Relationship\", PBR.\"ParentId\", " +
                "           PBR.\"Principal_Id\", PBR.\"PolicyHolderId\", PBR.\"BenefitId\", PBR.\"Benefit\", " +
                "           PBR.\"BenefitAllocatedAmount\", PBR.\"SHARED_BENEFIT\", PBR.\"CategoryName\", " +
                "           PBR.\"WaitingPeriodWaived\", PBR.\"ProductCategory\", PBR.\"PolicyStatus\", PBR.\"PolicyActive\", " +
                "           COALESCE(PC.\"ClaimAmount\", 0) AS \"ClaimAmount\", " +
                "           (COALESCE(PBR.\"BenefitAllocatedAmount\", 0) - COALESCE(PC.\"ClaimAmount\", 0) - COALESCE(PC.\"OS_PREAUTH_AMOUNT\", 0)) AS \"BALANCE\", " +
                "           COALESCE(PC.\"OS_PREAUTH_AMOUNT\", 0) AS \"OS_PREAUTH_AMOUNT\", PBR.\"Country\", PBR.\"Country_Name\" " +
                "    FROM CTE_POLICY_BENEFICIARY_BENEFITS PBR " +
                "    LEFT JOIN CTE_PAID_CLAIMS_SHARED_BENEFITS_SUMMARY PC ON PBR.\"PolicyId\" = PC.\"PolicyId\" " +
                "     AND PBR.\"EffectiveDate\" = PC.\"EffectiveDate\" " +
                "     AND PBR.\"BenefitId\" = PC.\"BenefitId\" " +
                "     AND PBR.\"Principal_Id\" = PC.\"Principal_Id\" " +
                "     AND PBR.\"Country\" = PC.\"Country\" " +
                "    WHERE PBR.\"SHARED_BENEFIT\" = 'Y' " +
                "), " +
                "CTE_FACT_BALANCES_MAIN_BENEFITS_GRAIN AS (" +
                "    SELECT * FROM CTE_BALANCES_BENEFICIARY_NON_SHARED_BENEFITS " +
                "    UNION ALL " +
                "    SELECT * FROM CTE_BALANCES_BENEFICIARY_SHARED_BENEFITS " +
                ") " +
                "SELECT * FROM CTE_FACT_BALANCES_MAIN_BENEFITS_GRAIN";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setLong(1, beneficiaryId);
                ps.setString(2, country.trim());

                try (ResultSet rs = ps.executeQuery()) {

                    MbElement root = outMessage.getRootElement();

                    // HTTP Headers
                    MbElement httpHeader = root.createElementAsLastChild("HTTPRequestHeader");
                    httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

                    // JSON Root Structure
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

                        long benefitId = rs.getLong("BenefitId");
                        String benefit = rs.getString("Benefit");
                        String beneficiaryName = rs.getString("BeneficiaryName");
                        long principalId = rs.getLong("Principal_Id");
                        
                        BigDecimal balanceVal = rs.getBigDecimal("BALANCE");
                        if (balanceVal == null) {
                            balanceVal = BigDecimal.ZERO;
                        }
                        balanceVal = balanceVal.setScale(2, RoundingMode.HALF_UP);
                        
                        String currency = rs.getString("Country");

                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "BenefitId", benefitId);
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Benefit", (benefit != null) ? benefit : "");
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "BeneficiaryName", (beneficiaryName != null) ? beneficiaryName : "");
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Principal_Id", principalId);
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Balance", balanceVal);
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Currency", (currency != null) ? currency : "KES");
                    }

                    if (hasRecords) {
                        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "responseCode", "SUCCESS");
                        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "status", "00");
                        out.propagate(outAssembly);
                    } else {
                        sendAlternateResponse(alternateMessage, outLocalEnv, 404, false, 
                            "No main benefit balances found matching beneficiaryId: " + beneficiaryIdStr + " and country: " + country);
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

    private String getCurrentTimestampIso() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TimeZone.getTimeZone("Africa/Nairobi"));
        return sdf.format(new Date());
    }
}