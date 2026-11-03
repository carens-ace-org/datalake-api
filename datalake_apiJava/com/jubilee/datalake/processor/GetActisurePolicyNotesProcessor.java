package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import java.sql.*;

public class GetActisurePolicyNotesProcessor extends MbJavaComputeNode {

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
            String policyIdStr = getPathParam(inAssembly, "policyId");
            int policyId;

            try {
                policyId = Integer.parseInt(policyIdStr);
            } catch (Exception e) {
                sendErrorResponse(alternateMessage, outLocalEnv, 400, "policyId must be a valid integer policy identifier.");
                alternate.propagate(alternateAssembly);
                return;
            }

            String sql = "SELECT * FROM `Actisure_Dim_Policy_Notes` WHERE `PolicyId` = ?";

            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, policyId);

                try (ResultSet rs = ps.executeQuery()) {
                    MbElement root = outMessage.getRootElement();
                    root.createElementAsLastChild("HTTPRequestHeader").createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

                    MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
                    MbElement arrayRoot = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
                    arrayRoot.setSpecificType(MbJSON.ARRAY);

                    ResultSetMetaData rsmd = rs.getMetaData();
                    int colCount = rsmd.getColumnCount();

                    while (rs.next()) {
                        MbElement item = arrayRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
                        item.setSpecificType(MbJSON.OBJECT);
                        for (int i = 1; i <= colCount; i++) {
                            Object val = rs.getObject(i);
                            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, rsmd.getColumnLabel(i), val != null ? val.toString() : null);
                        }
                    }
                    out.propagate(outAssembly);
                }
            }

        } catch (Exception e) {
            sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: " + e.getMessage());
            alternate.propagate(alternateAssembly);
        }
    }

    private String getPathParam(MbMessageAssembly assembly, String key) {
        try {
            MbElement el = assembly.getLocalEnvironment().getRootElement().getFirstElementByPath("REST/Input/Parameters/" + key);
            if (el != null && el.getValueAsString() != null) return el.getValueAsString().trim();
        } catch (Exception ignored) {}
        return null;
    }

    private void sendErrorResponse(MbMessage altMsg, MbMessage altEnv, int code, String msg) throws MbException {
        MbElement dest = altEnv.getRootElement().createElementAsLastChild(MbElement.TYPE_NAME, "Destination", null)
                                               .createElementAsLastChild(MbElement.TYPE_NAME, "HTTP", null);
        dest.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "ReplyStatusCode", code);

        MbElement root = altMsg.getRootElement();
        root.createElementAsLastChild("HTTPRequestHeader").createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        MbElement json = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", msg);
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "statusCode", code);
    }
}