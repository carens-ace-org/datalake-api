package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class GetSubBenefitBalanceRequestProcessor extends MbJavaComputeNode {

    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";
    
    private static final String SUB_BENEFIT_BALANCE_SQL = 
        "WITH CTE_POLICY_BENEFICIARY AS (" +
        "    SELECT ROW_NUMBER() OVER (PARTITION BY \"BeneficiaryId\" ORDER BY \"EffectiveDate\" DESC) AS \"ROW_NUM\", " +
        "           \"PolicyId\", \"EffectiveDate\", \"BeneficiaryId\", \"Principal_Id\", \"Country_Name\", \"Country\" " +
        "    FROM public.\"ACTISURE_POLICY_BENEFICIARIES\" " +
        "    WHERE \"BeneficiaryId\"::text = ? " +
        "      AND \"EffectiveDate\"::date <= CURRENT_DATE " +
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
        "           BN.\"BenefitId\", BN.\"Benefit\", BN.\"BenefitAllocatedAmount\", " +
        "           BN.\"SubBenefitId\", BN.\"SubBenefit\", BN.\"SubBenefitAllocatedAmount\", BN.\"SubBenefitFullRefund\", " +
        "           BN.\"BenefitDetailId\", BN.\"BenefitDetail\", BN.\"BenefitDetailAllocatedAmount\", " +
        "           BN.\"SHARED_BENEFIT\", BN.\"SHARED_SUB_BENEFIT\", BN.\"SHARED_BDL_BENEFIT\", " +
        "           BN.\"CategoryName\", BN.\"WaitingPeriodWaived\", BN.\"ProductCategory\", BN.\"PolicyStatus\", BN.\"PolicyActive\", " +
        "           BN.\"Country\", BN.\"Country_Name\" " +
        "    FROM public.\"ACTISURE_POLICY_BENEFITS\" BN " +
        "    JOIN public.\"ACTISURE_POLICY_BENEFICIARIES\" PB ON BN.\"PolicyId\" = PB.\"PolicyId\" " +
        "     AND BN.\"EffectiveDate\"::date = PB.\"EffectiveDate\"::date " +
        "     AND COALESCE(BN.\"Category\", -1) = COALESCE(PB.\"DivisionId\", -1) " +
        "     AND BN.\"Country\" = PB.\"Country\" " +
        "    JOIN CTE_LATEST_POLICY_BENEFICIARY LP ON PB.\"PolicyId\" = LP.\"PolicyId\" " +
        "     AND PB.\"EffectiveDate\"::date = LP.\"EffectiveDate\"::date " +
        "     AND PB.\"BeneficiaryId\" = LP.\"BeneficiaryId\" " +
        "     AND PB.\"Country\" = LP.\"Country\" " +
        "), " +
        "CTE_PAID_CLAIMS_DETAILED AS (" +
        "    SELECT PC.\"PolicyId\", PC.\"EffectiveDate\", PC.\"InvoiceBenefitID\" AS \"BenefitId\", PC.\"InvoiceBenefit\" AS \"Benefit\", " +
        "           PC.\"InvoiceSubBenefitID\" AS \"SubBenefitId\", PC.\"InvoiceLineDetailID\" AS \"BenefitDetailId\", " +
        "           PC.\"BeneficiaryId\", PB.\"Principal_Id\", PB.\"PolicyHolderId\", " +
        "           PC.\"SHARED_BENEFIT\", PC.\"SHARED_SUB_BENEFIT\", PC.\"SHARED_BDL_BENEFIT\", PC.\"ClaimAmount\", PC.\"Country\" " +
        "    FROM public.\"ACTISURE_PAID_CLAIMS\" PC " +
        "    JOIN public.\"ACTISURE_POLICY_BENEFICIARIES\" PB ON PC.\"PolicyId\" = PB.\"PolicyId\" " +
        "     AND PC.\"EffectiveDate\"::date = PB.\"EffectiveDate\"::date " +
        "     AND PC.\"BeneficiaryId\" = PB.\"BeneficiaryId\" " +
        "     AND PC.\"Country\" = PB.\"Country\" " +
        "    JOIN CTE_LATEST_POLICY_BENEFICIARY LP ON PB.\"PolicyId\" = LP.\"PolicyId\" " +
        "     AND PB.\"EffectiveDate\"::date = LP.\"EffectiveDate\"::date " +
        "     AND PB.\"Principal_Id\" = LP.\"Principal_Id\" " +
        "     AND PB.\"Country\" = LP.\"Country\" " +
        "), " +
        "CTE_PAID_CLAIMS_SUMMARY AS (" +
        "    SELECT CL.\"PolicyId\", CL.\"EffectiveDate\", CL.\"BenefitId\", CL.\"Benefit\", CL.\"SubBenefitId\", " +
        "           CL.\"BeneficiaryId\", CL.\"Principal_Id\", CL.\"PolicyHolderId\", CL.\"SHARED_SUB_BENEFIT\", " +
        "           SUM(CL.\"ClaimAmount\") AS \"ClaimAmount\", CL.\"Country\" " +
        "    FROM CTE_PAID_CLAIMS_DETAILED CL " +
        "    GROUP BY CL.\"PolicyId\", CL.\"EffectiveDate\", CL.\"BenefitId\", CL.\"Benefit\", CL.\"SubBenefitId\", " +
        "             CL.\"BeneficiaryId\", CL.\"Principal_Id\", CL.\"PolicyHolderId\", CL.\"SHARED_SUB_BENEFIT\", CL.\"Country\" " +
        "), " +
        "CTE_OUTSTANDING_PREAUTHS_SUMMARY AS (" +
        "    SELECT OP.\"PolicyId\", OP.\"EffectiveDate\", OP.\"BeneficiaryId\", OP.\"BenefitId\", OP.\"SubBenefitId\", " +
        "           SUM(OP.\"OS_PREAUTH_AMOUNT\") AS \"OS_PREAUTH_AMOUNT\", OP.\"Country\" " +
        "    FROM public.\"ACTISURE_OUTSTANDING_PREAUTHS\" OP " +
        "    JOIN CTE_LATEST_POLICY_BENEFICIARY LP ON OP.\"PolicyId\" = LP.\"PolicyId\" " +
        "     AND OP.\"EffectiveDate\"::date = LP.\"EffectiveDate\"::date " +
        "     AND OP.\"BeneficiaryId\" = LP.\"BeneficiaryId\" " +
        "     AND OP.\"Country\" = LP.\"Country\" " +
        "    GROUP BY OP.\"PolicyId\", OP.\"EffectiveDate\", OP.\"BeneficiaryId\", OP.\"BenefitId\", OP.\"SubBenefitId\", OP.\"Country\" " +
        "), " +
        "CTE_PAID_CLAIMS_NON_SHARED_BENEFITS_SUMMARY AS (" +
        "    SELECT PC.\"PolicyId\", PC.\"EffectiveDate\", PC.\"BeneficiaryId\", PC.\"BenefitId\", PC.\"Benefit\", " +
        "           PC.\"SubBenefitId\", PC.\"Principal_Id\", PC.\"PolicyHolderId\", PC.\"SHARED_SUB_BENEFIT\", PC.\"ClaimAmount\", " +
        "           COALESCE(OP.\"OS_PREAUTH_AMOUNT\", 0) AS \"OS_PREAUTH_AMOUNT\", PC.\"Country\" " +
        "    FROM CTE_PAID_CLAIMS_SUMMARY PC " +
        "    LEFT JOIN CTE_OUTSTANDING_PREAUTHS_SUMMARY OP ON PC.\"PolicyId\" = OP.\"PolicyId\" " +
        "     AND PC.\"EffectiveDate\"::date = OP.\"EffectiveDate\"::date " +
        "     AND PC.\"BeneficiaryId\" = OP.\"BeneficiaryId\" " +
        "     AND PC.\"SubBenefitId\" = OP.\"SubBenefitId\" " +
        "     AND PC.\"Country\" = OP.\"Country\" " +
        "    WHERE PC.\"SHARED_SUB_BENEFIT\" = 'N' " +
        "), " +
        "CTE_PAID_CLAIMS_SHARED_BENEFITS_SUMMARY AS (" +
        "    SELECT PC.\"PolicyId\", PC.\"EffectiveDate\", PC.\"BenefitId\", PC.\"Benefit\", PC.\"SubBenefitId\", " +
        "           PC.\"Principal_Id\", PC.\"PolicyHolderId\", PC.\"SHARED_SUB_BENEFIT\", " +
        "           SUM(PC.\"ClaimAmount\") AS \"ClaimAmount\", " +
        "           SUM(COALESCE(OP.\"OS_PREAUTH_AMOUNT\", 0)) AS \"OS_PREAUTH_AMOUNT\", PC.\"Country\" " +
        "    FROM CTE_PAID_CLAIMS_SUMMARY PC " +
        "    LEFT JOIN CTE_OUTSTANDING_PREAUTHS_SUMMARY OP ON PC.\"PolicyId\" = OP.\"PolicyId\" " +
        "     AND PC.\"EffectiveDate\"::date = OP.\"EffectiveDate\"::date " +
        "     AND PC.\"BeneficiaryId\" = OP.\"BeneficiaryId\" " +
        "     AND PC.\"SubBenefitId\" = OP.\"SubBenefitId\" " +
        "    WHERE PC.\"SHARED_SUB_BENEFIT\" = 'Y' " +
        "    GROUP BY PC.\"PolicyId\", PC.\"EffectiveDate\", PC.\"BenefitId\", PC.\"Benefit\", PC.\"SubBenefitId\", " +
        "             PC.\"Principal_Id\", PC.\"PolicyHolderId\", PC.\"SHARED_SUB_BENEFIT\", PC.\"Country\" " +
        "), " +
        "CTE_POLICY_BENEFICIARY_BENEFITS AS (" +
        "    SELECT \"PolicyId\", \"ParentPolicyId\", \"EffectiveDate\", \"BeneficiaryId\", \"BeneficiaryName\", " +
        "           \"Relationship\", \"ParentId\", \"Principal_Id\", \"PolicyHolderId\", \"BenefitId\", \"Benefit\", " +
        "           \"SubBenefitId\", \"SubBenefit\", \"SubBenefitAllocatedAmount\", \"SubBenefitFullRefund\", " +
        "           \"SHARED_SUB_BENEFIT\", \"CategoryName\", \"WaitingPeriodWaived\", \"ProductCategory\", " +
        "           \"PolicyStatus\", \"PolicyActive\", \"Country\", \"Country_Name\" " +
        "    FROM CTE_POLICY_BENEFICIARY_BENEFITS_DETAILED " +
        "    GROUP BY \"PolicyId\", \"ParentPolicyId\", \"EffectiveDate\", \"BeneficiaryId\", \"BeneficiaryName\", " +
        "             \"Relationship\", \"ParentId\", \"Principal_Id\", \"PolicyHolderId\", \"BenefitId\", \"Benefit\", " +
        "             \"SubBenefitId\", \"SubBenefit\", \"SubBenefitAllocatedAmount\", \"SubBenefitFullRefund\", " +
        "             \"SHARED_SUB_BENEFIT\", \"CategoryName\", \"WaitingPeriodWaived\", \"ProductCategory\", " +
        "             \"PolicyStatus\", \"PolicyActive\", \"Country\", \"Country_Name\" " +
        "), " +
        "CTE_BALANCES_BENEFICIARY_NON_SHARED_BENEFITS AS (" +
        "    SELECT PBR.\"PolicyId\", PBR.\"EffectiveDate\", PBR.\"ParentPolicyId\" AS \"MasterPolicyId\", " +
        "           PBR.\"BeneficiaryId\", PBR.\"BeneficiaryName\", PBR.\"Relationship\", PBR.\"ParentId\", " +
        "           PBR.\"Principal_Id\", PBR.\"PolicyHolderId\", PBR.\"BenefitId\", PBR.\"Benefit\", " +
        "           PBR.\"SubBenefitId\", PBR.\"SubBenefit\", PBR.\"SubBenefitAllocatedAmount\", PBR.\"SubBenefitFullRefund\", " +
        "           PBR.\"SHARED_SUB_BENEFIT\", PBR.\"CategoryName\", PBR.\"WaitingPeriodWaived\", " +
        "           PBR.\"ProductCategory\", PBR.\"PolicyStatus\", PBR.\"PolicyActive\", " +
        "           COALESCE(PC.\"ClaimAmount\", 0) AS \"ClaimAmount\", " +
        "           (COALESCE(PBR.\"SubBenefitAllocatedAmount\", 0) - COALESCE(PC.\"ClaimAmount\", 0) - COALESCE(PC.\"OS_PREAUTH_AMOUNT\", 0)) AS \"BALANCE\", " +
        "           COALESCE(PC.\"OS_PREAUTH_AMOUNT\", 0) AS \"OS_PREAUTH_AMOUNT\", PBR.\"Country\", PBR.\"Country_Name\" " +
        "    FROM CTE_POLICY_BENEFICIARY_BENEFITS PBR " +
        "    LEFT JOIN CTE_PAID_CLAIMS_NON_SHARED_BENEFITS_SUMMARY PC ON PBR.\"PolicyId\" = PC.\"PolicyId\" " +
        "     AND PBR.\"EffectiveDate\"::date = PC.\"EffectiveDate\"::date " +
        "     AND PBR.\"SubBenefitId\" = PC.\"SubBenefitId\" " +
        "     AND PBR.\"BeneficiaryId\" = PC.\"BeneficiaryId\" " +
        "     AND PBR.\"Principal_Id\" = PC.\"Principal_Id\" " +
        "     AND PBR.\"Country\" = PC.\"Country\" " +
        "    WHERE PBR.\"SHARED_SUB_BENEFIT\" = 'N' " +
        "), " +
        "CTE_BALANCES_BENEFICIARY_SHARED_BENEFITS AS (" +
        "    SELECT PBR.\"PolicyId\", PBR.\"EffectiveDate\", PBR.\"ParentPolicyId\" AS \"MasterPolicyId\", " +
        "           PBR.\"BeneficiaryId\", PBR.\"BeneficiaryName\", PBR.\"Relationship\", PBR.\"ParentId\", " +
        "           PBR.\"Principal_Id\", PBR.\"PolicyHolderId\", PBR.\"BenefitId\", PBR.\"Benefit\", " +
        "           PBR.\"SubBenefitId\", PBR.\"SubBenefit\", PBR.\"SubBenefitAllocatedAmount\", PBR.\"SubBenefitFullRefund\", " +
        "           PBR.\"SHARED_SUB_BENEFIT\", PBR.\"CategoryName\", PBR.\"WaitingPeriodWaived\", " +
        "           PBR.\"ProductCategory\", PBR.\"PolicyStatus\", PBR.\"PolicyActive\", " +
        "           COALESCE(PC.\"ClaimAmount\", 0) AS \"ClaimAmount\", " +
        "           (COALESCE(PBR.\"SubBenefitAllocatedAmount\", 0) - COALESCE(PC.\"ClaimAmount\", 0) - COALESCE(PC.\"OS_PREAUTH_AMOUNT\", 0)) AS \"BALANCE\", " +
        "           COALESCE(PC.\"OS_PREAUTH_AMOUNT\", 0) AS \"OS_PREAUTH_AMOUNT\", PBR.\"Country\", PBR.\"Country_Name\" " +
        "    FROM CTE_POLICY_BENEFICIARY_BENEFITS PBR " +
        "    LEFT JOIN CTE_PAID_CLAIMS_SHARED_BENEFITS_SUMMARY PC ON PBR.\"PolicyId\" = PC.\"PolicyId\" " +
        "     AND PBR.\"EffectiveDate\"::date = PC.\"EffectiveDate\"::date " +
        "     AND PBR.\"SubBenefitId\" = PC.\"SubBenefitId\" " +
        "     AND PBR.\"Principal_Id\" = PC.\"Principal_Id\" " +
        "     AND PBR.\"Country\" = PC.\"Country\" " +
        "    WHERE PBR.\"SHARED_SUB_BENEFIT\" = 'Y' " +
        "), " +
        "CTE_BALANCE AS (" +
        "    SELECT public.get_main_benefit_balance(?::integer, ?::integer) AS BALANCE " +
        "), " +
        "CTE_FACT_BALANCES_SUB_BENEFITS_GRAIN AS (" +
        "    SELECT BN.\"PolicyId\", BN.\"EffectiveDate\", BN.\"MasterPolicyId\", BN.\"BeneficiaryId\", BN.\"BeneficiaryName\", " +
        "           BN.\"Relationship\", BN.\"ParentId\", BN.\"Principal_Id\", BN.\"PolicyHolderId\", BN.\"BenefitId\", BN.\"Benefit\", " +
        "           BN.\"SubBenefitId\", BN.\"SubBenefit\", BN.\"SubBenefitAllocatedAmount\", BN.\"SubBenefitFullRefund\", " +
        "           BN.\"SHARED_SUB_BENEFIT\", BN.\"CategoryName\", BN.\"WaitingPeriodWaived\", BN.\"ProductCategory\", " +
        "           BN.\"PolicyStatus\", BN.\"PolicyActive\", BN.\"ClaimAmount\", " +
        "           CASE " +
        "               WHEN COALESCE(BN.\"SubBenefitFullRefund\", 0) = 0 THEN CAST(BN.\"BALANCE\" AS NUMERIC(18,2)) " +
        "               ELSE CAST(BL.BALANCE AS NUMERIC(18,2)) " +
        "           END AS \"BALANCE\", " +
        "           BL.BALANCE AS \"PARENT_BALANCE\", BN.\"OS_PREAUTH_AMOUNT\", BN.\"Country\", BN.\"Country_Name\" " +
        "    FROM CTE_BALANCES_BENEFICIARY_NON_SHARED_BENEFITS BN " +
        "    JOIN CTE_BALANCE BL ON 1 = 1 " +
        "    UNION ALL " +
        "    SELECT BN.\"PolicyId\", BN.\"EffectiveDate\", BN.\"MasterPolicyId\", BN.\"BeneficiaryId\", BN.\"BeneficiaryName\", " +
        "           BN.\"Relationship\", BN.\"ParentId\", BN.\"Principal_Id\", BN.\"PolicyHolderId\", BN.\"BenefitId\", BN.\"Benefit\", " +
        "           BN.\"SubBenefitId\", BN.\"SubBenefit\", BN.\"SubBenefitAllocatedAmount\", BN.\"SubBenefitFullRefund\", " +
        "           BN.\"SHARED_SUB_BENEFIT\", BN.\"CategoryName\", BN.\"WaitingPeriodWaived\", BN.\"ProductCategory\", " +
        "           BN.\"PolicyStatus\", BN.\"PolicyActive\", BN.\"ClaimAmount\", " +
        "           CASE " +
        "               WHEN COALESCE(BN.\"SubBenefitFullRefund\", 0) = 0 THEN CAST(BN.\"BALANCE\" AS NUMERIC(18,2)) " +
        "               ELSE CAST(BL.BALANCE AS NUMERIC(18,2)) " +
        "           END AS \"BALANCE\", " +
        "           BL.BALANCE AS \"PARENT_BALANCE\", BN.\"OS_PREAUTH_AMOUNT\", BN.\"Country\", BN.\"Country_Name\" " +
        "    FROM CTE_BALANCES_BENEFICIARY_SHARED_BENEFITS BN " +
        "    JOIN CTE_BALANCE BL ON 1 = 1 " +
        ") " +
        "SELECT * FROM CTE_FACT_BALANCES_SUB_BENEFITS_GRAIN " +
        "WHERE \"BenefitId\" = ?";

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
            String beneficiaryIdStr = Utils.getParam(inAssembly, "beneficiaryId", "BeneficiaryId", "entityId", "EntityID");
            String benefitIdStr = Utils.getParam(inAssembly, "benefitId", "BenefitId");
            String country = Utils.getParam(inAssembly, "country", "Country", "Country_Name");

            if (isEmpty(beneficiaryIdStr) || isEmpty(benefitIdStr) || isEmpty(country)) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Required query parameters missing: beneficiaryId, benefitId, and country must be provided.");
                alternate.propagate(alternateAssembly);
                return;
            }

            int beneficiaryIdInt;
            int benefitIdInt;
            long benefitId;
            long beneficiaryId;

            try {
                beneficiaryIdInt = Integer.parseInt(beneficiaryIdStr.trim());
                benefitIdInt = Integer.parseInt(benefitIdStr.trim());
                benefitId = Long.parseLong(benefitIdStr.trim());
                beneficiaryId = Long.parseLong(beneficiaryIdStr.trim());
            } catch (NumberFormatException nfe) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Invalid beneficiaryId or benefitId parameter format. Must be numeric.");
                alternate.propagate(alternateAssembly);
                return;
            }

            executeQueryAndBuildResponse(outMessage, alternateMessage, outLocalEnv, out, alternate,
                                         outAssembly, alternateAssembly, beneficiaryIdStr, benefitIdStr, country.trim(),
                                         beneficiaryIdInt, benefitIdInt, benefitId, beneficiaryId);

        } catch (Exception e) {
            System.err.println("Database Error: " + e.getMessage());
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: There was a problem establishing a connection to the given database URL.");
            alternate.propagate(alternateAssembly);
        }
    }

    private boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    private void executeQueryAndBuildResponse(MbMessage outMessage, MbMessage alternateMessage, MbMessage outLocalEnv,
                                              MbOutputTerminal out, MbOutputTerminal alternate,
                                              MbMessageAssembly outAssembly, MbMessageAssembly alternateAssembly,
                                              String beneficiaryIdStr, String benefitIdStr, String country,
                                              int beneficiaryIdInt, int benefitIdInt, long benefitId, long beneficiaryId) throws Exception {
        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(SUB_BENEFIT_BALANCE_SQL)) {

            ps.setString(1, beneficiaryIdStr);
            ps.setString(2, country);
            ps.setInt(3, beneficiaryIdInt);
            ps.setInt(4, benefitIdInt);
            ps.setLong(5, benefitId);

            try (ResultSet rs = ps.executeQuery()) {
                boolean hasRecords = buildJsonPayload(outMessage, rs);

                if (hasRecords) {
                    out.propagate(outAssembly);
                } else {
                    Utils.sendErrorResponse(alternateMessage, outLocalEnv, 404, 
                        "No sub benefit balances found matching beneficiaryId: " + beneficiaryId + ", benefitId: " + benefitId + ", and country: " + country);
                    alternate.propagate(alternateAssembly);
                }
            }
        }
    }

    private boolean buildJsonPayload(MbMessage outMessage, ResultSet rs) throws Exception {
        MbElement root = outMessage.getRootElement();

        MbElement httpHeader = root.createElementAsLastChild("HTTPRequestHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", true);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", "Member data retrieved successfully");
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "statusCode", 200);

        MbElement dataArray = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "data", null);
        dataArray.setSpecificType(MbJSON.ARRAY);

        boolean hasRecords = false;

        while (rs.next()) {
            hasRecords = true;

            MbElement itemObj = dataArray.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
            itemObj.setSpecificType(MbJSON.OBJECT);

            long currentBenefitId = rs.getLong("BenefitId");
            String subBenefit = rs.getString("SubBenefit");
            BigDecimal balanceVal = rs.getBigDecimal("BALANCE");
            if (balanceVal == null) {
                balanceVal = BigDecimal.ZERO;
            }
            balanceVal = balanceVal.setScale(2, RoundingMode.HALF_UP);
            String currency = rs.getString("Country");

            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "BenefitId", currentBenefitId);
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "SubBenefit", (subBenefit != null) ? subBenefit : "");
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Balance", balanceVal);
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Currency", (currency != null) ? currency : "KES");
        }

        if (hasRecords) {
            jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "responseCode", "SUCCESS");
            jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "status", "00");
        }

        return hasRecords;
    }
}