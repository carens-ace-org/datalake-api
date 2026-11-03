package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class GetPolicyDetailsRequestProcessor extends MbJavaComputeNode {

    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";
    
    private static final String POLICY_DETAILS_SQL = 
        "SELECT fp.\"PolicyID\", " +
        "fp.\"Policy_Status\", " +
        "fp.\"Policy_Active\", " +
        "fp.\"Product_ID\", " +
        "db.\"Product\", " +
        "fp.\"Effective_Date\" " +
        "FROM public.\"Actisure_Fact_Policy\" fp " +
        "JOIN public.\"Actisure_Dim_Product\" db ON db.\"Product_ID\" = fp.\"Product_ID\" " +
        "WHERE fp.\"PolicyID\" = ? " +
        "AND fp.\"Country_Name\" = ?";

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
            String policyIdStr = Utils.getParam(inAssembly, "policyId", "PolicyId", "PolicyID");
            String countryRaw = Utils.getParam(inAssembly, "country", "Country", "Country_Name");

            if (isEmpty(policyIdStr) || isEmpty(countryRaw)) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Required query parameters missing: policyId and country must be provided.");
                alternate.propagate(alternateAssembly);
                return;
            }

            String country = countryRaw.trim().toUpperCase();

            long policyId;
            try {
                policyId = Long.parseLong(policyIdStr.trim());
            } catch (NumberFormatException nfe) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Invalid PolicyID format. Must be a numeric integer.");
                alternate.propagate(alternateAssembly);
                return;
            }

            executeQueryAndBuildResponse(outMessage, alternateMessage, outLocalEnv, out, alternate,
                                         outAssembly, alternateAssembly, policyId, country, policyIdStr);

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
                                              long policyId, String country, String policyIdStr) throws Exception {
        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(POLICY_DETAILS_SQL)) {

            ps.setLong(1, policyId);
            ps.setString(2, country);

            try (ResultSet rs = ps.executeQuery()) {
                boolean hasRecords = buildJsonPayload(outMessage, rs, country);

                if (hasRecords) {
                    out.propagate(outAssembly);
                } else {
                    Utils.sendErrorResponse(alternateMessage, outLocalEnv, 404, 
                        "No policy details found for policyId: " + policyIdStr + " and country: " + country);
                    alternate.propagate(alternateAssembly);
                }
            }
        }
    }

    private boolean buildJsonPayload(MbMessage outMessage, ResultSet rs, String country) throws Exception {
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

            long currentPolicyId = rs.getLong("PolicyID");
            String policyStatus = rs.getString("Policy_Status");
            String product = rs.getString("Product");
            String schemeName = (product != null) ? product + " Scheme" : "";

            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "PolicyID", currentPolicyId);
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Policy_Status", (policyStatus != null) ? policyStatus : "");
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Product", (product != null) ? product : "");
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Scheme_Name", schemeName);
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Country", country);
        }

        if (hasRecords) {
            jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "responseCode", "SUCCESS");
            jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "status", "00");
        }

        return hasRecords;
    }
}