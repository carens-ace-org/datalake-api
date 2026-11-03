package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class GetFirstBuyTransactionProcessor extends MbJavaComputeNode {

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
            // 1. Extract Query Parameter: accountNumber
            String accountNumber = getParam(inAssembly, "accountNumber");
            
            accountNumber = accountNumber.trim();

            // 2. Validate Parameter
            if (accountNumber == null || accountNumber.trim().isEmpty()) {
                sendErrorResponse(alternateMessage, outLocalEnv, 400, "Missing required query parameter: accountNumber is mandatory.");
                alternate.propagate(alternateAssembly);
                return;
            }

            // 3. Construct CTE Query for First Buy Transaction
            StringBuilder sql = new StringBuilder();
            sql.append("WITH CTE_TRANSACTIONS AS ( ");
            sql.append("    SELECT ");
            sql.append("        ROW_NUMBER() OVER (PARTITION BY REPLACE(account_number, '-1', '') ORDER BY date ASC) AS ROW_NUM, ");
            sql.append("        REPLACE(account_number, '-1', '') AS account_number, ");
            sql.append("        transaction_id, ");
            sql.append("        amount, ");
            sql.append("        units, ");
            sql.append("        price, ");
            sql.append("        date, ");
            sql.append("        description, ");
            sql.append("        sub_type ");
            sql.append("    FROM public.jaml_transactions ");
            sql.append("    WHERE sub_type = 'Buy' ");
            sql.append(") ");
            sql.append("SELECT ");
            sql.append("    account_number AS \"accountNumber\", ");
            sql.append("    transaction_id AS \"transactionId\", ");
            sql.append("    amount AS \"amount\", ");
            sql.append("    units AS \"units\", ");
            sql.append("    price AS \"price\", ");
            sql.append("    date AS \"date\", ");
            sql.append("    description AS \"description\", ");
            sql.append("    sub_type AS \"subType\" ");
            sql.append("FROM CTE_TRANSACTIONS ");
            sql.append("WHERE ROW_NUM = 1 AND account_number = ?");

            // 4.Execute Query
            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);

            try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                ps.setString(1, accountNumber.trim());

                try (ResultSet rs = ps.executeQuery()) {
                    MbElement root = outMessage.getRootElement();

                    // Response Header
                    MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
                    httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

                    SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

                    if (rs.next()) {
                        // Set Top-Level Single JSON Object
                        MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
                        jsonRoot.setSpecificType(MbJSON.OBJECT);

                        ResultSetMetaData rsmd = rs.getMetaData();
                        int columnCount = rsmd.getColumnCount();

                        for (int i = 1; i <= columnCount; i++) {
                            String colName = rsmd.getColumnLabel(i);
                            Object colValue = rs.getObject(i);

                            // Omit null values completely from JSON payload output
                            if (colValue == null) {
                                continue;
                            }

                            if (colValue instanceof Number) {
                                jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue);
                            } else if (colValue instanceof Boolean) {
                                jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue);
                            } else if (colValue instanceof Timestamp) {
                                jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, isoFormat.format((Timestamp) colValue));
                            } else {
                                jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue.toString());
                            }
                        }

                        out.propagate(outAssembly);
                    } else {
                        // Return 404 Not Found if no Buy transaction exists for account
                        sendErrorResponse(alternateMessage, outLocalEnv, 404, "No Buy transaction found for accountNumber: " + accountNumber);
                        alternate.propagate(alternateAssembly);
                    }
                }
            }

        } catch (Exception e) {
            sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: " + e.getMessage());
            alternate.propagate(alternateAssembly);
        }
    }

    private String getParam(MbMessageAssembly assembly, String key) {
        try {
            MbElement root = assembly.getLocalEnvironment().getRootElement();
            MbElement el = root.getFirstElementByPath("REST/Input/Parameters/" + key);
            if (el != null && el.getValueAsString() != null) return el.getValueAsString().trim();

            el = root.getFirstElementByPath("HTTP/Input/QueryString/" + key);
            if (el != null && el.getValueAsString() != null) return el.getValueAsString().trim();
        } catch (Exception ignored) {}
        return null;
    }

    private void sendErrorResponse(MbMessage altMsg, MbMessage altEnv, int code, String msg) throws MbException {
        MbElement localEnvRoot = altEnv.getRootElement();
        MbElement dest = localEnvRoot.getFirstElementByPath("Destination");
        if (dest == null) {
            dest = localEnvRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Destination", null);
        }

        MbElement http = dest.getFirstElementByPath("HTTP");
        if (http == null) {
            http = dest.createElementAsLastChild(MbElement.TYPE_NAME, "HTTP", null);
        }

        MbElement replyCode = http.getFirstElementByPath("ReplyStatusCode");
        if (replyCode == null) {
            http.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "ReplyStatusCode", code);
        } else {
            replyCode.setValue(code);
        }

        MbElement root = altMsg.getRootElement();
        MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        MbElement json = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        json.setSpecificType(MbJSON.OBJECT);
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", false);
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", msg);
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "statusCode", code);
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "responseCode", "ERROR");
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "status", "99");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TimeZone.getTimeZone("Africa/Nairobi"));
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "timestamp", sdf.format(new Date()));
    }
}