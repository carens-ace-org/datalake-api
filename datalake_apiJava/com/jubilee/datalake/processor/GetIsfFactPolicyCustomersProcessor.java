package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;

import java.sql.*;
import java.text.SimpleDateFormat;

public class GetIsfFactPolicyCustomersProcessor extends MbJavaComputeNode {

    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.S";
    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";
    
    private static final String POLICY_CUSTOMERS_SQL = 
        "SELECT * FROM \"ISF_Fact_Policy\" " +
        "WHERE CAST(%s AS DATE) >= ?::date " +
        "  AND CAST(%s AS DATE) < ?::date";

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

            if (isEmpty(fromDate) || isEmpty(toDate) || isEmpty(status)) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Missing required query parameters: fromDate, toDate, and status are mandatory.");
                alternate.propagate(alternateAssembly);
                return;
            }

            String cleanStatus = status.trim();
            if (!"NEW".equalsIgnoreCase(cleanStatus) && !"UPDATED".equalsIgnoreCase(cleanStatus)) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Invalid status parameter. Must be either 'NEW' or 'UPDATED'.");
                alternate.propagate(alternateAssembly);
                return;
            }

            String filterColumn = "NEW".equalsIgnoreCase(cleanStatus) ? "\"LOAD_DATE\"" : "\"UPDATE_DATE\"";

            executeQueryAndBuildResponse(outMessage, filterColumn, fromDate.trim(), toDate.trim());
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

    private void executeQueryAndBuildResponse(MbMessage outMessage, String filterColumn, String fromDate, String toDate) throws Exception {
        String sql = String.format(POLICY_CUSTOMERS_SQL, filterColumn, filterColumn);

        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, fromDate);
            ps.setString(2, toDate);

            try (ResultSet rs = ps.executeQuery()) {
                buildJsonPayload(outMessage, rs);
            }
        }
    }

    private void buildJsonPayload(MbMessage outMessage, ResultSet rs) throws Exception {
        MbElement root = outMessage.getRootElement();

        MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        jsonRoot.setSpecificType(MbJSON.ARRAY);

        populateResultSetRows(rs, jsonRoot);
    }

    private void populateResultSetRows(ResultSet rs, MbElement jsonRoot) throws Exception {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        SimpleDateFormat tsFormat = new SimpleDateFormat(TIMESTAMP_FORMAT);

        while (rs.next()) {
            MbElement item = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
            item.setSpecificType(MbJSON.OBJECT);
            mapRowColumns(rs, rsmd, columnCount, item, tsFormat);
        }
    }

    private void mapRowColumns(ResultSet rs, ResultSetMetaData rsmd, int columnCount, MbElement item, SimpleDateFormat tsFormat) throws Exception {
        for (int i = 1; i <= columnCount; i++) {
            String colName = rsmd.getColumnLabel(i);
            Object val = rs.getObject(i);

            if (val == null) {
                continue;
            }

            if (val instanceof Number || val instanceof Boolean) {
                item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, val);
            } else if (val instanceof Timestamp) {
                item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, tsFormat.format((Timestamp) val));
            } else {
                item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, val.toString());
            }
        }
    }
}