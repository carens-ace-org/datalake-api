package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import java.sql.*;

public class GetIsfFactPolicyCustomersPaginatedProcessor extends MbJavaComputeNode {

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
            String fromDate = getParam(inAssembly, "fromDate");
            String toDate = getParam(inAssembly, "toDate");
            String status = getParam(inAssembly, "status");
            String productCode = getParam(inAssembly, "productCode");

            if (fromDate == null || toDate == null || status == null || 
               (!status.equals("NEW") && !status.equals("UPDATED"))) {
                sendErrorResponse(alternateMessage, outLocalEnv, 400, "fromDate, toDate, and status are required. status must be NEW or UPDATED.");
                alternate.propagate(alternateAssembly);
                return;
            }

            int page = getIntParam(inAssembly, "page", 0);
            int size = getIntParam(inAssembly, "size", 400);
            int offset = page * size;

            String dateCol = status.equals("NEW") ? "LOAD_DATE" : "UPDATE_DATE";

            String sql = "SELECT p.PRODUCT_NAME, m.* " +
                         "FROM `ISF_Fact_Policy` m " +
                         "JOIN `ISF_Dim_Product` p ON CONCAT(m.PRODUCT_CODE) = CONCAT(p.PRODUCT_CODE) " +
                         "WHERE DATE(m.`" + dateCol + "`) >= ? AND DATE(m.`" + dateCol + "`) < ? " +
                         "  AND (? IS NULL OR p.PRODUCT_CODE = ?) " +
                         "ORDER BY m.LIFE_ASSURED_REF_NO " +
                         "LIMIT ? OFFSET ?";

            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, fromDate);
                ps.setString(2, toDate);
                ps.setString(3, productCode);
                ps.setString(4, productCode);
                ps.setInt(5, size);
                ps.setInt(6, offset);

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

    private String getParam(MbMessageAssembly assembly, String key) {
        try {
            MbElement el = assembly.getLocalEnvironment().getRootElement().getFirstElementByPath("REST/Input/Parameters/" + key);
            if (el == null) el = assembly.getLocalEnvironment().getRootElement().getFirstElementByPath("HTTP/Input/QueryString/" + key);
            if (el != null && el.getValueAsString() != null) return el.getValueAsString().trim();
        } catch (Exception ignored) {}
        return null;
    }

    private int getIntParam(MbMessageAssembly assembly, String key, int defVal) {
        try {
            String valStr = getParam(assembly, key);
            if (valStr != null) return Integer.parseInt(valStr);
        } catch (Exception ignored) {}
        return defVal;
    }
}