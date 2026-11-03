package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class GetCardTypesRequestProcessor extends MbJavaComputeNode {

    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";
    private static final String CARD_TYPES_SQL = 
        "SELECT \"CardTypeId\", \"CardTypeName\", \"Country\" " +
        "FROM \"Actisure_Dim_CardTypes\" " +
        "WHERE UPPER(TRIM(\"Country\")) = UPPER(TRIM(?))";

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
            String country = Utils.getParam(inAssembly, "country", "Country", "Country_Name");

            if (country == null || country.trim().isEmpty()) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Required query parameter missing: country must be provided.");
                alternate.propagate(alternateAssembly);
                return;
            }

            List<String> cardTypes = executeQuery(country.trim());

            if (!cardTypes.isEmpty()) {
                buildJsonPayload(outMessage, cardTypes);
                out.propagate(outAssembly);
            } else {
                String errorMsg = "No card types found for country: " + country;
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 404, errorMsg);
                alternate.propagate(alternateAssembly);
            }

        } catch (Exception e) {
            System.err.println("Database Error: " + e.getMessage());
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: " + e.getMessage());
            alternate.propagate(alternateAssembly);
        }
    }

    private List<String> executeQuery(String country) throws Exception {
        List<String> list = new ArrayList<>();
        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(CARD_TYPES_SQL)) {

            ps.setString(1, country);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String cardTypeName = rs.getString("CardTypeName");
                    list.add(cardTypeName != null ? cardTypeName : "");
                }
            }
        }
        return list;
    }

    private void buildJsonPayload(MbMessage outMessage, List<String> cardTypes) throws Exception {
        MbElement root = outMessage.getRootElement();

        MbElement httpHeader = root.createElementAsLastChild("HTTPRequestHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        jsonRoot.setSpecificType(MbJSON.ARRAY);

        for (String cardType : cardTypes) {
            jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Item", cardType);
        }
    }
}