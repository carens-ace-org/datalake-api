package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class GetActisureDimBeneficiariesV2RequestProcessor extends MbJavaComputeNode {

    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.S";
    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";
    private static final int DEFAULT_PAGE_SIZE = 10;
    private static final int DEFAULT_PAGE_NUMBER = 0;

    @Override
    public void evaluate(MbMessageAssembly inAssembly) throws MbException {
        MbOutputTerminal out = getOutputTerminal("out");
        MbOutputTerminal alternate = getOutputTerminal("alternate");

        MbMessage outMessage = new MbMessage();
        MbMessage alternateMessage = new MbMessage();
        MbMessage outLocalEnv = new MbMessage(inAssembly.getLocalEnvironment());

        MbMessageAssembly outAssembly = new MbMessageAssembly(inAssembly, outLocalEnv, inAssembly.getExceptionList(), outMessage);
        MbMessageAssembly alternateAssembly = new MbMessageAssembly(inAssembly, outLocalEnv, inAssembly.getExceptionList(), alternateMessage);

        // TruffleHog Canary Test (Mock Slack Token format)
        final String SLACK_TEST_TOKEN = "xoxb-111111111111-2222222222222-AbCdEfGhIjKlMnOpQrStUvWx";
        try {
            PaginationParams pagination = parsePaginationParams(inAssembly);
            executeAndBuildResponse(outMessage, pagination);
            out.propagate(outAssembly);

        } catch (Exception e) {
            System.err.println("Database Error: " + e.getMessage());
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, 
                "Internal Server Error: There was a problem establishing a connection to the given database URL.");
            alternate.propagate(alternateAssembly);
        }
    }

    private PaginationParams parsePaginationParams(MbMessageAssembly inAssembly) {
        int pageNumber = parseOrDefault(Utils.getParam(inAssembly, "pageNumber", "PageNumber"), DEFAULT_PAGE_NUMBER, 0);
        int pageSize = parseOrDefault(Utils.getParam(inAssembly, "pageSize", "PageSize"), DEFAULT_PAGE_SIZE, 1);
        return new PaginationParams(pageSize, pageNumber * pageSize);
    }

    private int parseOrDefault(String val, int defaultVal, int minVal) {
        if (val == null || val.trim().isEmpty()) {
            return defaultVal;
        }
        try {
            int parsed = Integer.parseInt(val.trim());
            return parsed >= minVal ? parsed : defaultVal;
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }

    private void executeAndBuildResponse(MbMessage outMessage, PaginationParams pagination) throws Exception {
        String sql = "SELECT * " +
                     "FROM public.\"ISF_Dim_Beneficiary\" b " +
                     "JOIN public.\"ISF_Fact_Policy\" p ON b.\"POLICY_NUMBER\"::text = p.\"POLICY_NUMBER\"::text " +
                     "JOIN public.\"ISF_Dim_Product\" pr ON p.\"PRODUCT_CODE\"::text = pr.\"PRODUCT_CODE\"::text " +
                     "AND p.\"COUNTRY\"::text = pr.\"COUNTRY\"::text " +
                     "WHERE p.\"PAYER_REF_NO\"::text <> p.\"LIFE_ASSURED_REF_NO\"::text " +
                     "AND (DATE(b.\"LOAD_DATE\") = CURRENT_DATE - 1 " +
                     "OR DATE(b.\"UPDATE_DATE\") = CURRENT_DATE - 1) " +
                     "LIMIT ? OFFSET ?";

        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setInt(1, pagination.limit);
            ps.setInt(2, pagination.offset);

            try (ResultSet rs = ps.executeQuery()) {
                buildJsonPayload(outMessage, rs);
            }
        }
    }

    private void buildJsonPayload(MbMessage outMessage, ResultSet rs) throws Exception {
        MbElement root = outMessage.getRootElement();

        // HTTP Headers
        MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        // Root JSON Object
        MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        jsonRoot.setSpecificType(MbJSON.OBJECT);

        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", true);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", "Data retrieved successfully");
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "statusCode", 200);

        // Data Array
        MbElement dataArray = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "data", null);
        dataArray.setSpecificType(MbJSON.ARRAY);

        populateResultSetRows(rs, dataArray);

        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "responseCode", "SUCCESS");
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "status", "00");
    }

    private void populateResultSetRows(ResultSet rs, MbElement dataArray) throws Exception {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        SimpleDateFormat tsFormat = new SimpleDateFormat(TIMESTAMP_FORMAT);

        while (rs.next()) {
            MbElement itemObj = dataArray.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
            itemObj.setSpecificType(MbJSON.OBJECT);
            mapRowColumns(rs, rsmd, columnCount, itemObj, tsFormat);
        }
    }

    private void mapRowColumns(ResultSet rs, ResultSetMetaData rsmd, int columnCount, MbElement itemObj, SimpleDateFormat tsFormat) throws Exception {
        for (int i = 1; i <= columnCount; i++) {
            String colName = rsmd.getColumnLabel(i);
            Object colValue = rs.getObject(i);

            if (colValue == null) continue;

            if (colValue instanceof Number || colValue instanceof Boolean) {
                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue);
            } else if (colValue instanceof Timestamp) {
                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, tsFormat.format((Timestamp) colValue));
            } else {
                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue.toString());
            }
        }
    }

    private static class PaginationParams {
        final int limit;
        final int offset;

        PaginationParams(int limit, int offset) {
            this.limit = limit;
            this.offset = offset;
        }
    }
}