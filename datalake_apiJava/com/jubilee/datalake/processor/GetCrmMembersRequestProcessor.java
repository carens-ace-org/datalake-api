package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class GetCrmMembersRequestProcessor extends MbJavaComputeNode {

    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";
    private static final String CRM_MEMBERS_SQL = 
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
            String beneficiaryIdStr = Utils.getParam(inAssembly, "beneficiaryId", "BeneficiaryId", "BENEFICIARY_ID");
            String country = Utils.getParam(inAssembly, "country", "Country", "Country_Name");

            if (isEmpty(beneficiaryIdStr) || isEmpty(country)) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Required query parameters missing: beneficiaryId and country must be provided.");
                alternate.propagate(alternateAssembly);
                return;
            }

            long beneficiaryId;
            try {
                beneficiaryId = Long.parseLong(beneficiaryIdStr.trim());
            } catch (NumberFormatException nfe) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Invalid beneficiaryId format. Must be a numeric value.");
                alternate.propagate(alternateAssembly);
                return;
            }

            List<MemberRecord> members = executeQuery(beneficiaryId, country.trim());

            if (!members.isEmpty()) {
                buildJsonPayload(outMessage, members);
                out.propagate(outAssembly);
            } else {
                String errorMsg = "No member records found for beneficiaryId: " + beneficiaryIdStr + " and country: " + country;
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

    private List<MemberRecord> executeQuery(long beneficiaryId, String country) throws Exception {
        List<MemberRecord> list = new ArrayList<>();
        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(CRM_MEMBERS_SQL)) {

            ps.setLong(1, beneficiaryId);
            ps.setString(2, country);
            ps.setString(3, country);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long principalId = rs.getLong("Principal_Id");
                    boolean isPrincipalNull = rs.wasNull();
                    String beneficiaryName = rs.getString("BeneficiaryName");
                    String principalName = rs.getString("PrincipalName");
                    String schemeName = rs.getString("Scheme_Name");
                    String countryName = rs.getString("Country_Name");

                    list.add(new MemberRecord(
                        isPrincipalNull ? null : principalId,
                        beneficiaryName != null ? beneficiaryName : "",
                        principalName != null ? principalName : "",
                        schemeName != null ? schemeName : "",
                        countryName != null ? countryName : ""
                    ));
                }
            }
        }
        return list;
    }

    private void buildJsonPayload(MbMessage outMessage, List<MemberRecord> members) throws Exception {
        MbElement root = outMessage.getRootElement();

        MbElement httpHeader = root.createElementAsLastChild("HTTPRequestHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        jsonRoot.setSpecificType(MbJSON.ARRAY);

        for (MemberRecord member : members) {
            MbElement itemObj = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
            itemObj.setSpecificType(MbJSON.OBJECT);

            if (member.principalId != null) {
                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Principal_Id", member.principalId);
            }
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "BeneficiaryName", member.beneficiaryName);
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "PrincipalName", member.principalName);
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Scheme_Name", member.schemeName);
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Country_Name", member.countryName);
        }
    }

    private static class MemberRecord {
        final Long principalId;
        final String beneficiaryName;
        final String principalName;
        final String schemeName;
        final String countryName;

        MemberRecord(Long principalId, String beneficiaryName, String principalName, String schemeName, String countryName) {
            this.principalId = principalId;
            this.beneficiaryName = beneficiaryName;
            this.principalName = principalName;
            this.schemeName = schemeName;
            this.countryName = countryName;
        }
    }
}