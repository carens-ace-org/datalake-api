package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class GetMembersRequestProcessor extends MbJavaComputeNode {

    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";
    
    private static final String MEMBERS_SQL = 
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

            executeQueryAndBuildResponse(outMessage, alternateMessage, outLocalEnv, out, alternate,
                                         outAssembly, alternateAssembly, beneficiaryId, country.trim(), beneficiaryIdStr);

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
                                              long beneficiaryId, String country, String beneficiaryIdStr) throws Exception {
        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(MEMBERS_SQL)) {

            ps.setLong(1, beneficiaryId);
            ps.setString(2, country);
            ps.setString(3, country);

            try (ResultSet rs = ps.executeQuery()) {
                boolean hasRecords = buildJsonPayload(outMessage, rs, beneficiaryId, country);

                if (hasRecords) {
                    out.propagate(outAssembly);
                } else {
                    Utils.sendErrorResponse(alternateMessage, outLocalEnv, 404, 
                        "No member records found matching beneficiaryId: " + beneficiaryIdStr + " and country: " + country);
                    alternate.propagate(alternateAssembly);
                }
            }
        }
    }

    private boolean buildJsonPayload(MbMessage outMessage, ResultSet rs, long beneficiaryId, String country) throws Exception {
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

            String smartNo = getColumnString(rs, "SMART_NO", String.valueOf(beneficiaryId));
            long principalId = rs.getLong("Principal_Id");
            String beneficiaryName = getColumnString(rs, "BeneficiaryName", "");
            String principalName = getColumnString(rs, "PrincipalName", beneficiaryName);
            String idNumber = getColumnString(rs, "ID_Number", "");
            String schemeName = getColumnString(rs, "Scheme_Name", "");
            String countryName = getColumnString(rs, "Country_Name", country);

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
        }

        return hasRecords;
    }

    private String getColumnString(ResultSet rs, String columnName, String defaultValue) {
        try {
            String val = rs.getString(columnName);
            return (val != null && !val.trim().isEmpty()) ? val.trim() : defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }
}