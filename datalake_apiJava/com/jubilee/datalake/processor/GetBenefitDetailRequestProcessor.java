package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.model.BenefitDetail;
import com.jubilee.datalake.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class GetBenefitDetailRequestProcessor extends MbJavaComputeNode {

    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";
    private static final String BENEFIT_DETAIL_SQL = 
        "WITH CTE_POLICY_BENEFICIARY AS (" +
        "    SELECT ROW_NUMBER() OVER (PARTITION BY \"BeneficiaryId\" ORDER BY \"EffectiveDate\" DESC) AS \"ROW_NUM\", " +
        "           \"PolicyId\", \"EffectiveDate\", \"BeneficiaryId\", \"Principal_Id\", \"Country_Name\", \"Country\" " +
        "    FROM \"ACTISURE_POLICY_BENEFICIARIES\" " +
        "    WHERE \"BeneficiaryId\" = ? " +
        "      AND \"EffectiveDate\" <= CURRENT_TIMESTAMP " +
        "      AND \"Country_Name\" = ? " +
        "), " +
        "CTE_LATEST_POLICY_BENEFICIARY AS (" +
        "    SELECT \"PolicyId\", \"BeneficiaryId\", \"Principal_Id\", \"EffectiveDate\", \"Country_Name\", \"Country\" " +
        "    FROM CTE_POLICY_BENEFICIARY WHERE \"ROW_NUM\" = 1 " +
        "), " +
        "CTE_POLICY_BENEFICIARY_BENEFITS_DETAILED AS (" +
        "    SELECT BN.\"PolicyId\", BN.\"ParentPolicyId\", BN.\"Category\" AS \"EnterpriseCategory\", BN.\"EffectiveDate\", " +
        "           PB.\"BeneficiaryId\", PB.\"BeneficiaryName\", PB.\"Relationship\", PB.\"ParentId\", PB.\"Principal_Id\", PB.\"PolicyHolderId\", " +
        "           BN.\"BenefitId\", BN.\"Benefit\", BN.\"BenefitAllocatedAmount\", BN.\"SubBenefitId\", BN.\"SubBenefit\", BN.\"SubBenefitAllocatedAmount\", " +
        "           BN.\"SubBenefitFullRefund\", BN.\"BenefitDetailId\", BN.\"BenefitDetail\", BN.\"BenefitDetailWaitingPeriod\", " +
        "           BN.\"BenefitDetailAllocatedAmount\", BN.\"BenefitDetailFullRefund\", BN.\"SHARED_BENEFIT\", BN.\"SHARED_SUB_BENEFIT\", " +
        "           BN.\"SHARED_BDL_BENEFIT\", BN.\"CategoryName\", BN.\"WaitingPeriodWaived\", BN.\"ProductCategory\", BN.\"PolicyStatus\", " +
        "           BN.\"PolicyActive\", BN.\"Country\", BN.\"Country_Name\" " +
        "    FROM \"ACTISURE_POLICY_BENEFITS\" BN " +
        "    JOIN \"ACTISURE_POLICY_BENEFICIARIES\" PB ON BN.\"PolicyId\" = PB.\"PolicyId\" " +
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
        "           PC.\"InvoiceSubBenefitID\" AS \"SubBenefitId\", PC.\"InvoiceLineDetailID\" AS \"BenefitDetailId\", PC.\"BeneficiaryId\", " +
        "           PB.\"Principal_Id\", PB.\"PolicyHolderId\", PC.\"SHARED_BENEFIT\", PC.\"SHARED_SUB_BENEFIT\", PC.\"SHARED_BDL_BENEFIT\", " +
        "           PC.\"ClaimAmount\", PC.\"Country\" " +
        "    FROM \"ACTISURE_PAID_CLAIMS\" PC " +
        "    JOIN \"ACTISURE_POLICY_BENEFICIARIES\" PB ON PC.\"PolicyId\" = PB.\"PolicyId\" " +
        "     AND PC.\"EffectiveDate\" = PB.\"EffectiveDate\" " +
        "     AND PC.\"BeneficiaryId\" = PB.\"BeneficiaryId\" " +
        "    JOIN CTE_LATEST_POLICY_BENEFICIARY LP ON PB.\"PolicyId\" = LP.\"PolicyId\" " +
        "     AND PB.\"EffectiveDate\" = LP.\"EffectiveDate\" " +
        "     AND PB.\"Principal_Id\" = LP.\"Principal_Id\" " +
        "     AND PB.\"Country\" = LP.\"Country\" " +
        "     AND PC.\"Country\" = PB.\"Country\" " +
        "), " +
        "CTE_PAID_CLAIMS_SUMMARY AS (" +
        "    SELECT CL.\"PolicyId\", CL.\"EffectiveDate\", CL.\"BenefitId\", CL.\"Benefit\", CL.\"SubBenefitId\", CL.\"BenefitDetailId\", " +
        "           CL.\"BeneficiaryId\", CL.\"Principal_Id\", CL.\"PolicyHolderId\", CL.\"SHARED_BDL_BENEFIT\", SUM(CL.\"ClaimAmount\") AS \"ClaimAmount\", CL.\"Country\" " +
        "    FROM CTE_PAID_CLAIMS_DETAILED CL " +
        "    GROUP BY CL.\"PolicyId\", CL.\"EffectiveDate\", CL.\"BenefitId\", CL.\"Benefit\", CL.\"SubBenefitId\", CL.\"BenefitDetailId\", " +
        "             CL.\"BeneficiaryId\", CL.\"Principal_Id\", CL.\"PolicyHolderId\", CL.\"SHARED_BDL_BENEFIT\", CL.\"Country\" " +
        "), " +
        "CTE_OUTSTANDING_PREAUTHS_SUMMARY AS (" +
        "    SELECT OP.\"PolicyId\", OP.\"EffectiveDate\", OP.\"BeneficiaryId\", OP.\"BenefitId\", OP.\"SubBenefitId\", OP.\"BenefitDetailId\", " +
        "           SUM(OP.\"OS_PREAUTH_AMOUNT\") AS \"OS_PREAUTH_AMOUNT\", OP.\"Country\" " +
        "    FROM \"ACTISURE_OUTSTANDING_PREAUTHS\" OP " +
        "    JOIN CTE_LATEST_POLICY_BENEFICIARY LP ON OP.\"PolicyId\" = LP.\"PolicyId\" " +
        "     AND OP.\"EffectiveDate\" = LP.\"EffectiveDate\" " +
        "     AND OP.\"BeneficiaryId\" = LP.\"BeneficiaryId\" " +
        "     AND OP.\"Country\" = LP.\"Country\" " +
        "    GROUP BY OP.\"PolicyId\", OP.\"EffectiveDate\", OP.\"BeneficiaryId\", OP.\"BenefitId\", OP.\"SubBenefitId\", OP.\"BenefitDetailId\", OP.\"Country\" " +
        "), " +
        "CTE_PAID_CLAIMS_NON_SHARED_BENEFITS_SUMMARY AS (" +
        "    SELECT PC.\"PolicyId\", PC.\"EffectiveDate\", PC.\"BeneficiaryId\", PC.\"BenefitId\", PC.\"Benefit\", PC.\"SubBenefitId\", PC.\"BenefitDetailId\", " +
        "           PC.\"Principal_Id\", PC.\"PolicyHolderId\", PC.\"SHARED_BDL_BENEFIT\", PC.\"ClaimAmount\", COALESCE(OP.\"OS_PREAUTH_AMOUNT\", 0) AS \"OS_PREAUTH_AMOUNT\", PC.\"Country\" " +
        "    FROM CTE_PAID_CLAIMS_SUMMARY PC " +
        "    LEFT JOIN CTE_OUTSTANDING_PREAUTHS_SUMMARY OP ON PC.\"PolicyId\" = OP.\"PolicyId\" " +
        "     AND PC.\"EffectiveDate\" = OP.\"EffectiveDate\" " +
        "     AND PC.\"BeneficiaryId\" = OP.\"BeneficiaryId\" " +
        "     AND PC.\"BenefitDetailId\" = OP.\"BenefitDetailId\" " +
        "     AND PC.\"Country\" = OP.\"Country\" " +
        "    WHERE PC.\"SHARED_BDL_BENEFIT\" = 'N' " +
        "), " +
        "CTE_PAID_CLAIMS_SHARED_BENEFITS_SUMMARY AS (" +
        "    SELECT PC.\"PolicyId\", PC.\"EffectiveDate\", PC.\"BenefitId\", PC.\"Benefit\", PC.\"SubBenefitId\", PC.\"BenefitDetailId\", PC.\"Principal_Id\", " +
        "           PC.\"PolicyHolderId\", PC.\"SHARED_BDL_BENEFIT\", SUM(PC.\"ClaimAmount\") AS \"ClaimAmount\", SUM(COALESCE(OP.\"OS_PREAUTH_AMOUNT\", 0)) AS \"OS_PREAUTH_AMOUNT\", PC.\"Country\" " +
        "    FROM CTE_PAID_CLAIMS_SUMMARY PC " +
        "    LEFT JOIN CTE_OUTSTANDING_PREAUTHS_SUMMARY OP ON PC.\"PolicyId\" = OP.\"PolicyId\" " +
        "     AND PC.\"EffectiveDate\" = OP.\"EffectiveDate\" " +
        "     AND PC.\"BeneficiaryId\" = OP.\"BeneficiaryId\" " +
        "     AND PC.\"BenefitDetailId\" = OP.\"BenefitDetailId\" " +
        "    WHERE PC.\"SHARED_BDL_BENEFIT\" = 'Y' " +
        "    GROUP BY PC.\"PolicyId\", PC.\"EffectiveDate\", PC.\"BenefitId\", PC.\"Benefit\", PC.\"SubBenefitId\", PC.\"BenefitDetailId\", PC.\"Principal_Id\", PC.\"PolicyHolderId\", PC.\"SHARED_BDL_BENEFIT\", PC.\"Country\" " +
        "), " +
        "CTE_POLICY_BENEFICIARY_BENEFITS AS (" +
        "    SELECT \"PolicyId\", \"ParentPolicyId\", \"EffectiveDate\", \"BeneficiaryId\", \"BeneficiaryName\", \"Relationship\", \"ParentId\", \"Principal_Id\", " +
        "           \"PolicyHolderId\", \"BenefitId\", \"Benefit\", \"SubBenefitId\", \"SubBenefit\", \"SubBenefitFullRefund\", \"BenefitDetailId\", \"BenefitDetail\", " +
        "           \"BenefitDetailFullRefund\", \"BenefitDetailWaitingPeriod\", \"BenefitDetailAllocatedAmount\", \"SHARED_BDL_BENEFIT\", \"CategoryName\", " +
        "           \"WaitingPeriodWaived\", \"ProductCategory\", \"PolicyStatus\", \"PolicyActive\", \"Country\", \"Country_Name\" " +
        "    FROM CTE_POLICY_BENEFICIARY_BENEFITS_DETAILED " +
        "    GROUP BY \"PolicyId\", \"ParentPolicyId\", \"EffectiveDate\", \"BeneficiaryId\", \"BeneficiaryName\", \"Relationship\", \"ParentId\", \"Principal_Id\", " +
        "             \"PolicyHolderId\", \"BenefitId\", \"Benefit\", \"SubBenefitId\", \"SubBenefit\", \"SubBenefitFullRefund\", \"BenefitDetailId\", \"BenefitDetail\", " +
        "             \"BenefitDetailFullRefund\", \"BenefitDetailWaitingPeriod\", \"BenefitDetailAllocatedAmount\", \"SHARED_BDL_BENEFIT\", \"CategoryName\", " +
        "             \"WaitingPeriodWaived\", \"ProductCategory\", \"PolicyStatus\", \"PolicyActive\", \"Country\", \"Country_Name\" " +
        "), " +
        "CTE_BALANCES_BENEFICIARY_NON_SHARED_BENEFITS AS (" +
        "    SELECT PBR.\"PolicyId\", PBR.\"EffectiveDate\", PBR.\"ParentPolicyId\" AS \"MasterPolicyId\", PBR.\"BeneficiaryId\", PBR.\"BeneficiaryName\", PBR.\"Relationship\", " +
        "           PBR.\"ParentId\", PBR.\"Principal_Id\", PBR.\"PolicyHolderId\", PBR.\"BenefitId\", PBR.\"Benefit\", PBR.\"SubBenefitId\", PBR.\"SubBenefit\", PBR.\"SubBenefitFullRefund\", " +
        "           PBR.\"BenefitDetailId\", PBR.\"BenefitDetail\", PBR.\"BenefitDetailFullRefund\", PBR.\"BenefitDetailWaitingPeriod\", PBR.\"BenefitDetailAllocatedAmount\", " +
        "           PBR.\"SHARED_BDL_BENEFIT\", PBR.\"CategoryName\", PBR.\"WaitingPeriodWaived\", PBR.\"ProductCategory\", PBR.\"PolicyStatus\", PBR.\"PolicyActive\", " +
        "           COALESCE(PC.\"ClaimAmount\", 0) AS \"ClaimAmount\", (COALESCE(PBR.\"BenefitDetailAllocatedAmount\", 0) - COALESCE(PC.\"ClaimAmount\", 0)) AS \"BALANCE\", " +
        "           COALESCE(PC.\"OS_PREAUTH_AMOUNT\", 0) AS \"OS_PREAUTH_AMOUNT\", PBR.\"Country\", PBR.\"Country_Name\" " +
        "    FROM CTE_POLICY_BENEFICIARY_BENEFITS PBR " +
        "    LEFT JOIN CTE_PAID_CLAIMS_NON_SHARED_BENEFITS_SUMMARY PC ON PBR.\"PolicyId\" = PC.\"PolicyId\" " +
        "     AND PBR.\"EffectiveDate\" = PC.\"EffectiveDate\" " +
        "     AND PBR.\"BenefitDetailId\" = PC.\"BenefitDetailId\" " +
        "     AND PBR.\"BeneficiaryId\" = PC.\"BeneficiaryId\" " +
        "     AND PBR.\"Principal_Id\" = PC.\"Principal_Id\" " +
        "     AND PBR.\"Country\" = PC.\"Country\" " +
        "    WHERE PBR.\"SHARED_BDL_BENEFIT\" = 'N' " +
        "), " +
        "CTE_BALANCES_BENEFICIARY_SHARED_BENEFITS AS (" +
        "    SELECT PBR.\"PolicyId\", PBR.\"EffectiveDate\", PBR.\"ParentPolicyId\" AS \"MasterPolicyId\", PBR.\"BeneficiaryId\", PBR.\"BeneficiaryName\", PBR.\"Relationship\", " +
        "           PBR.\"ParentId\", PBR.\"Principal_Id\", PBR.\"PolicyHolderId\", PBR.\"BenefitId\", PBR.\"Benefit\", PBR.\"SubBenefitId\", PBR.\"SubBenefit\", PBR.\"SubBenefitFullRefund\", " +
        "           PBR.\"BenefitDetailId\", PBR.\"BenefitDetail\", PBR.\"BenefitDetailFullRefund\", PBR.\"BenefitDetailWaitingPeriod\", PBR.\"BenefitDetailAllocatedAmount\", " +
        "           PBR.\"SHARED_BDL_BENEFIT\", PBR.\"CategoryName\", PBR.\"WaitingPeriodWaived\", PBR.\"ProductCategory\", PBR.\"PolicyStatus\", PBR.\"PolicyActive\", " +
        "           COALESCE(PC.\"ClaimAmount\", 0) AS \"ClaimAmount\", (COALESCE(PBR.\"BenefitDetailAllocatedAmount\", 0) - COALESCE(PC.\"ClaimAmount\", 0)) AS \"BALANCE\", " +
        "           COALESCE(PC.\"OS_PREAUTH_AMOUNT\", 0) AS \"OS_PREAUTH_AMOUNT\", PBR.\"Country\", PBR.\"Country_Name\" " +
        "    FROM CTE_POLICY_BENEFICIARY_BENEFITS PBR " +
        "    LEFT JOIN CTE_PAID_CLAIMS_SHARED_BENEFITS_SUMMARY PC ON PBR.\"PolicyId\" = PC.\"PolicyId\" " +
        "     AND PBR.\"EffectiveDate\" = PC.\"EffectiveDate\" " +
        "     AND PBR.\"BenefitDetailId\" = PC.\"BenefitDetailId\" " +
        "     AND PBR.\"Principal_Id\" = PC.\"Principal_Id\" " +
        "     AND PBR.\"Country\" = PC.\"Country\" " +
        "    WHERE PBR.\"SHARED_BDL_BENEFIT\" = 'Y' " +
        "), " +
        "CTE_BALANCE AS (" +
        "    SELECT get_sub_benefit_balance(?::integer, ?::integer, ?::integer) AS BALANCE " +
        "), " +
        "CTE_FACT_BALANCES_BENEFITS_DETAIL_GRAIN AS (" +
        "    (SELECT \"PolicyId\", \"EffectiveDate\", \"MasterPolicyId\", \"BeneficiaryId\", \"BeneficiaryName\", \"Relationship\", \"ParentId\", \"Principal_Id\", " +
        "            \"PolicyHolderId\", \"BenefitId\", \"Benefit\", \"SubBenefitId\", \"SubBenefit\", \"SubBenefitFullRefund\", \"BenefitDetailId\", \"BenefitDetail\", " +
        "            \"BenefitDetailFullRefund\", \"BenefitDetailWaitingPeriod\", \"BenefitDetailAllocatedAmount\", \"SHARED_BDL_BENEFIT\", \"CategoryName\", " +
        "            \"WaitingPeriodWaived\", \"ProductCategory\", \"PolicyStatus\", \"PolicyActive\", \"ClaimAmount\", " +
        "            CASE WHEN COALESCE(BN.\"BenefitDetailFullRefund\", 0) = 0 THEN CAST(BN.\"BALANCE\" AS DECIMAL(18,2)) ELSE CAST(BL.BALANCE AS DECIMAL(18,2)) END AS \"BALANCE\", " +
        "            BL.BALANCE AS \"PARENT_BALANCE\", \"OS_PREAUTH_AMOUNT\", \"Country\", \"Country_Name\" " +
        "     FROM CTE_BALANCES_BENEFICIARY_NON_SHARED_BENEFITS BN JOIN CTE_BALANCE BL ON 1 = 1) " +
        "    UNION " +
        "    (SELECT \"PolicyId\", \"EffectiveDate\", \"MasterPolicyId\", \"BeneficiaryId\", \"BeneficiaryName\", \"Relationship\", \"ParentId\", \"Principal_Id\", " +
        "            \"PolicyHolderId\", \"BenefitId\", \"Benefit\", \"SubBenefitId\", \"SubBenefit\", \"SubBenefitFullRefund\", \"BenefitDetailId\", \"BenefitDetail\", " +
        "            \"BenefitDetailFullRefund\", \"BenefitDetailWaitingPeriod\", \"BenefitDetailAllocatedAmount\", \"SHARED_BDL_BENEFIT\", \"CategoryName\", " +
        "            \"WaitingPeriodWaived\", \"ProductCategory\", \"PolicyStatus\", \"PolicyActive\", \"ClaimAmount\", " +
        "            CASE WHEN COALESCE(BN.\"BenefitDetailFullRefund\", 0) = 0 THEN CAST(BN.\"BALANCE\" AS DECIMAL(18,2)) ELSE CAST(BL.BALANCE AS DECIMAL(18,2)) END AS \"BALANCE\", " +
        "            BL.BALANCE AS \"PARENT_BALANCE\", \"OS_PREAUTH_AMOUNT\", \"Country\", \"Country_Name\" " +
        "     FROM CTE_BALANCES_BENEFICIARY_SHARED_BENEFITS BN JOIN CTE_BALANCE BL ON 1 = 1) " +
        ") " +
        "SELECT * FROM CTE_FACT_BALANCES_BENEFITS_DETAIL_GRAIN WHERE \"SubBenefitId\" = ?";

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
            String benefitId = Utils.getParam(inAssembly, "benefitId", "BenefitId");
            String subBenefitId = Utils.getParam(inAssembly, "subBenefitId", "SubBenefitId");

            if (isEmpty(beneficiaryId) || isEmpty(country) || isEmpty(benefitId) || isEmpty(subBenefitId)) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "All required query parameters must be provided: beneficiaryId, country, benefitId, subBenefitId.");
                alternate.propagate(alternateAssembly);
                return;
            }

            int intBeneficiaryId, intBenefitId, intSubBenefitId;
            try {
                intBeneficiaryId = Integer.parseInt(beneficiaryId.trim());
                intBenefitId = Integer.parseInt(benefitId.trim());
                intSubBenefitId = Integer.parseInt(subBenefitId.trim());
            } catch (NumberFormatException nfe) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Invalid numeric format for beneficiaryId, benefitId, or subBenefitId.");
                alternate.propagate(alternateAssembly);
                return;
            }

            List<BenefitDetail> detailsList = executeQuery(intBeneficiaryId, country.trim(), intBenefitId, intSubBenefitId);

            if (!detailsList.isEmpty()) {
                buildJsonPayload(outMessage, detailsList);
                out.propagate(outAssembly);
            } else {
                String errorMsg = String.format("No benefit detail records found matching beneficiaryId: %s, country: %s, benefitId: %s, subBenefitId: %s",
                        beneficiaryId, country, benefitId, subBenefitId);
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 404, errorMsg);
                alternate.propagate(alternateAssembly);
            }

        } catch (Exception e) {
            System.err.println("Database Error: " + e.getMessage());
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: There was a problem establishing a connection to the given database URL.");
            alternate.propagate(alternateAssembly);
        }
    }

    private boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    private List<BenefitDetail> executeQuery(int beneficiaryId, String country, int benefitId, int subBenefitId) throws Exception {
        List<BenefitDetail> list = new ArrayList<>();
        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(BENEFIT_DETAIL_SQL)) {

            ps.setInt(1, beneficiaryId);
            ps.setString(2, country);
            ps.setInt(3, beneficiaryId);
            ps.setInt(4, benefitId);
            ps.setInt(5, subBenefitId);
            ps.setInt(6, subBenefitId);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    list.add(BenefitDetail.fromResultSet(rs));
                }
            }
        }
        return list;
    }

    private void buildJsonPayload(MbMessage outMessage, List<BenefitDetail> detailsList) throws Exception {
        MbElement root = outMessage.getRootElement();

        MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        jsonRoot.setSpecificType(MbJSON.ARRAY);

        for (BenefitDetail item : detailsList) {
            MbElement itemObj = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
            itemObj.setSpecificType(MbJSON.OBJECT);

            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "SubBenefitId", item.getSubBenefitId());
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "SubBenefitName", item.getSubBenefitName());
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Limit", item.getLimit());
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Utilised", item.getUtilised());
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Available", item.getAvailable());
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Currency", item.getCurrency());
        }
    }
}