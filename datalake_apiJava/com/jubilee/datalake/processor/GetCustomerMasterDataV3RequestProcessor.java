package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class GetCustomerMasterDataV3RequestProcessor extends MbJavaComputeNode {

    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";
    
    private static final String MEMBER_SQL = 
        "WITH CTE_LATEST_POLICY AS (" +
        "  SELECT ROW_NUMBER() OVER (PARTITION BY \"PolicyId\" ORDER BY \"EffectiveDate\" DESC) AS ROW_NUM, " +
        "         \"PolicyId\", \"EffectiveDate\", \"BeneficiaryId\" " +
        "  FROM \"ACTISURE_POLICY_BENEFICIARIES\" " +
        "  WHERE 1 = 1 " +
        "    AND \"EffectiveDate\" <= CURRENT_TIMESTAMP " +
        "    AND \"Country_Name\" = ? " +
        "    AND \"BeneficiaryId\" = CAST(? AS BIGINT)" +
        "), " +
        "CTE_MEMBER AS (" +
        "  SELECT PB.\"PolicyId\", PB.\"EffectiveDate\", PB.\"BeneficiaryId\", PB.\"BeneficiaryName\", " +
        "         PB.\"ID_Number\", PB.\"OriginalJoinDate\", PB.\"ParentId\", PB.\"Relationship\", " +
        "         PB.\"Principal_Id\", PB.\"PrincipalName\", PB.\"PolicyHolderId\", PB.\"DOB\", " +
        "         PB.\"PhoneNumber\", PB.\"MOBILE_NUMBER_OTP\", PB.\"Email1\", PB.\"Email2\", " +
        "         PB.\"CardType\", PB.\"VIP\", PB.\"Elite\", PB.\"Country_Name\", SC.\"Scheme_Name\" " +
        "  FROM CTE_LATEST_POLICY PL " +
        "  JOIN \"ACTISURE_POLICY_BENEFICIARIES\" PB ON PL.\"PolicyId\" = PB.\"PolicyId\" " +
        "   AND PL.\"EffectiveDate\" = PB.\"EffectiveDate\" " +
        "   AND PL.\"BeneficiaryId\" = PB.\"BeneficiaryId\" " +
        "   AND PL.ROW_NUM = 1 " +
        "  LEFT JOIN \"Actisure_Dim_Scheme\" SC ON CAST(PB.\"PolicyHolderId\" AS VARCHAR) = CAST(SC.\"Scheme_ID\" AS VARCHAR) " +
        "   AND PB.\"Country\" = SC.\"Country\" " +
        "  WHERE PB.\"EffectiveDate\" <= CURRENT_TIMESTAMP " +
        "    AND PB.\"Country_Name\" = ?" +
        ") " +
        "SELECT * FROM CTE_MEMBER";

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
            String entityId = Utils.getParam(inAssembly, "EntityID", "entityId", "entityID");
            String country = Utils.getParam(inAssembly, "country", "Country", "Country_Name");

            if (isEmpty(entityId) || isEmpty(country)) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Missing mandatory query parameters: 'EntityID' and 'country' are required.");
                alternate.propagate(alternateAssembly);
                return;
            }

            long beneficiaryId;
            try {
                beneficiaryId = Long.parseLong(entityId.trim());
            } catch (NumberFormatException e) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Invalid 'EntityID': must be a valid numeric value.");
                alternate.propagate(alternateAssembly);
                return;
            }

            List<MemberRecord> members = executeQuery(country.trim(), beneficiaryId);

            if (!members.isEmpty()) {
                buildJsonPayload(outMessage, members);
                out.propagate(outAssembly);
            } else {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 404, "No member records found for EntityID: " + entityId + " and country: " + country);
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

    private List<MemberRecord> executeQuery(String country, long beneficiaryId) throws Exception {
        List<MemberRecord> list = new ArrayList<>();
        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement stmt = conn.prepareStatement(MEMBER_SQL)) {

            stmt.setString(1, country);
            stmt.setLong(2, beneficiaryId);
            stmt.setString(3, country);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String email = rs.getString("Email1");
                    if (email == null) email = rs.getString("Email2");

                    String phone = rs.getString("PhoneNumber");
                    if (phone == null) phone = rs.getString("MOBILE_NUMBER_OTP");

                    list.add(new MemberRecord(
                        rs.getString("BeneficiaryId"),
                        rs.getString("ID_Number"),
                        rs.getString("PrincipalName"),
                        rs.getString("BeneficiaryName"),
                        email,
                        phone,
                        rs.getString("DOB"),
                        rs.getString("Country_Name"),
                        rs.getString("Scheme_Name")
                    ));
                }
            }
        }
        return list;
    }

    private void buildJsonPayload(MbMessage outMessage, List<MemberRecord> members) throws Exception {
        MbElement root = outMessage.getRootElement();

        MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        jsonRoot.setSpecificType(MbJSON.OBJECT);

        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", true);

        MbElement statusObj = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "status", null);
        statusObj.setSpecificType(MbJSON.OBJECT);
        statusObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "code", 200);
        statusObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", "OK");

        MbElement detailsArray = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "details", null);
        detailsArray.setSpecificType(MbJSON.ARRAY);

        for (MemberRecord member : members) {
            MbElement item = detailsArray.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
            item.setSpecificType(MbJSON.OBJECT);

            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "beneficiaryId", member.beneficiaryId);
            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "nationalId", member.nationalId);
            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "principalName", member.principalName);
            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "beneficiaryName", member.beneficiaryName);
            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "taxNumber", null);
            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "gender", "Male");
            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "email", member.email);
            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "cellphone", member.cellphone);
            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "mobileNumber", member.cellphone);
            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "dob", member.dob);
            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "country", member.country);
            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "schemeName", member.schemeName);
        }
    }

    private static class MemberRecord {
        final String beneficiaryId;
        final String nationalId;
        final String principalName;
        final String beneficiaryName;
        final String email;
        final String cellphone;
        final String dob;
        final String country;
        final String schemeName;

        MemberRecord(String beneficiaryId, String nationalId, String principalName, String beneficiaryName, 
                     String email, String cellphone, String dob, String country, String schemeName) {
            this.beneficiaryId = beneficiaryId;
            this.nationalId = nationalId;
            this.principalName = principalName;
            this.beneficiaryName = beneficiaryName;
            this.email = email;
            this.cellphone = cellphone;
            this.dob = dob;
            this.country = country;
            this.schemeName = schemeName;
        }
    }
}