package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;

import java.sql.*;
import java.text.SimpleDateFormat;

public class GetFirstBuyTransactionProcessor extends MbJavaComputeNode {

    private static final String ISO_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";
    
    private static final String FIRST_BUY_SQL = 
        "WITH CTE_TRANSACTIONS AS (" +
        "    SELECT " +
        "        ROW_NUMBER() OVER (PARTITION BY REPLACE(account_number, '-1', '') ORDER BY date ASC) AS ROW_NUM, " +
        "        REPLACE(account_number, '-1', '') AS account_number, " +
        "        transaction_id, " +
        "        amount, " +
        "        units, " +
        "        price, " +
        "        date, " +
        "        description, " +
        "        sub_type " +
        "    FROM public.jaml_transactions " +
        "    WHERE sub_type = 'Buy'" +
        ") " +
        "SELECT " +
        "    account_number AS \"accountNumber\", " +
        "    transaction_id AS \"transactionId\", " +
        "    amount AS \"amount\", " +
        "    units AS \"units\", " +
        "    price AS \"price\", " +
        "    date AS \"date\", " +
        "    description AS \"description\", " +
        "    sub_type AS \"subType\" " +
        "FROM CTE_TRANSACTIONS " +
        "WHERE ROW_NUM = 1 AND account_number = ?";

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
            String accountNumber = Utils.getParam(inAssembly, "accountNumber", "AccountNumber");

            if (accountNumber == null || accountNumber.trim().isEmpty()) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Missing required query parameter: accountNumber is mandatory.");
                alternate.propagate(alternateAssembly);
                return;
            }

            String cleanAccount = accountNumber.trim();
            boolean hasRecord = executeQueryAndBuildResponse(outMessage, cleanAccount);

            if (hasRecord) {
                out.propagate(outAssembly);
            } else {
                String errorMsg = "No Buy transaction found for accountNumber: " + cleanAccount;
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 404, errorMsg);
                alternate.propagate(alternateAssembly);
            }

        } catch (Exception e) {
            System.err.println("Database Error: " + e.getMessage());
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: There was a problem establishing a connection to the given database URL.");
            alternate.propagate(alternateAssembly);
        }
    }

    private boolean executeQueryAndBuildResponse(MbMessage outMessage, String accountNumber) throws Exception {
        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(FIRST_BUY_SQL)) {

            ps.setString(1, accountNumber);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    buildJsonPayload(outMessage, rs);
                    return true;
                }
            }
        }
        return false;
    }

    private void buildJsonPayload(MbMessage outMessage, ResultSet rs) throws Exception {
        MbElement root = outMessage.getRootElement();

        MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        jsonRoot.setSpecificType(MbJSON.OBJECT);

        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        SimpleDateFormat isoFormat = new SimpleDateFormat(ISO_TIMESTAMP_FORMAT);

        for (int i = 1; i <= columnCount; i++) {
            String colName = rsmd.getColumnLabel(i);
            Object colValue = rs.getObject(i);

            if (colValue == null) {
                continue;
            }

            if (colValue instanceof Number || colValue instanceof Boolean) {
                jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue);
            } else if (colValue instanceof Timestamp) {
                jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, isoFormat.format((Timestamp) colValue));
            } else {
                jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue.toString());
            }
        }
    }
}