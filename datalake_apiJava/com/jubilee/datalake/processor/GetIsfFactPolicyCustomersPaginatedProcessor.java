package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;

import java.sql.*;

public class GetIsfFactPolicyCustomersPaginatedProcessor extends MbJavaComputeNode {

    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";
    
    private static final String POLICY_CUSTOMERS_SQL = 
        "SELECT p.PRODUCT_NAME, m.* " +
        "FROM \"ISF_Fact_Policy\" m " +
        "JOIN \"ISF_Dim_Product\" p ON CONCAT(m.PRODUCT_CODE) = CONCAT(p.PRODUCT_CODE) " +
        "WHERE DATE(m.\"%s\") >= ? AND DATE(m.\"%s\") < ? " +
        "  AND (? IS NULL OR p.PRODUCT_CODE = ?) " +
        "ORDER BY m.LIFE_ASSURED_REF_NO " +
        "LIMIT ? OFFSET ?";

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
            String fromDate = Utils.getParam(inAssembly, "fromDate", "FromDate");
            String toDate = Utils.getParam(inAssembly, "toDate", "ToDate");
            String status = Utils.getParam(inAssembly, "status", "Status");
            String productCode = Utils.getParam(inAssembly, "productCode", "ProductCode");

            if (isEmpty(fromDate) || isEmpty(toDate) || isEmpty(status) || 
                (!status.equals("NEW") && !status.equals("UPDATED"))) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "fromDate, toDate, and status are required. status must be NEW or UPDATED.");
                alternate.propagate(alternateAssembly);
                return;
            }

            int page = getIntParam(inAssembly, "page", 0);
            int size = getIntParam(inAssembly, "size", 400);
            int offset = page * size;

            String dateCol = status.equals("NEW") ? "LOAD_DATE" : "UPDATE_DATE";

            executeQueryAndBuildResponse(outMessage, dateCol, fromDate.trim(), toDate.trim(), productCode, size, offset);
            out.propagate(outAssembly);

        } catch (SQLException e) {
            System.err.println("Database Error: " + e.getMessage());
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: There was a problem establishing a connection to the given database URL.");
            alternate.propagate(alternateAssembly);
        } catch (Exception e) {
            System.err.println("Internal Error: " + e.getMessage());
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: " + e.getMessage());
            alternate.propagate(alternateAssembly);
        }
    }

    private boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    private int getIntParam(MbMessageAssembly assembly, String key, int defVal) {
        try {
            String valStr = Utils.getParam(assembly, key);
            if (valStr != null) {
                return Integer.parseInt(valStr.trim());
            }
        } catch (Exception ignored) {}
        return defVal;
    }

    private void executeQueryAndBuildResponse(MbMessage outMessage, String dateCol, String fromDate, String toDate, String productCode, int size, int offset) throws Exception {
        String sql = String.format(POLICY_CUSTOMERS_SQL, dateCol, dateCol);

        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, fromDate);
            ps.setString(2, toDate);
            ps.setString(3, productCode);
            ps.setString(4, productCode);
            ps.setInt(5, size);
            ps.setInt(6, offset);

            try (ResultSet rs = ps.executeQuery()) {
                buildJsonPayload(outMessage, rs);
            }
        }
    }

    private void buildJsonPayload(MbMessage outMessage, ResultSet rs) throws Exception {
        MbElement root = outMessage.getRootElement();

        MbElement httpHeader = root.createElementAsLastChild("HTTPRequestHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        MbElement arrayRoot = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
        arrayRoot.setSpecificType(MbJSON.ARRAY);

        ResultSetMetaData rsmd = rs.getMetaData();
        int colCount = rsmd.getColumnCount();

        while (rs.next()) {
            MbElement item = arrayRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
            item.setSpecificType(MbJSON.OBJECT);

            for (int i = 1; i <= colCount; i++) {
                String colName = rsmd.getColumnLabel(i);
                Object val = rs.getObject(i);
                item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, val != null ? val.toString() : null);
            }
        }
    }
}