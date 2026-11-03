package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.model.QueryParams;
import com.jubilee.datalake.utils.Utils;

import java.sql.*;
import java.text.SimpleDateFormat;

public class GetDaDimCustomersProcessor extends MbJavaComputeNode {

    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.S";
    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";

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
            QueryParams params = Utils.extractAndValidateParams(inAssembly);
            if (params.hasError()) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, params.getErrorCode(), params.getErrorMessage());
                alternate.propagate(alternateAssembly);
                return;
            }

            executeQueryAndBuildResponse(outMessage, params);
            out.propagate(outAssembly);

        } catch (Exception e) {
            System.err.println("Database Error: " + e.getMessage());
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: " + e.getMessage());
            alternate.propagate(alternateAssembly);
        }
    }

    private void executeQueryAndBuildResponse(MbMessage outMessage, QueryParams params) throws Exception {
        String dateColumn = "NEW".equals(params.status) ? "\"_date_loaded\"" : "\"_date_changed\"";

        String sql = "SELECT * FROM \"DA_Dim_Customer\" " +
                     "WHERE TO_TIMESTAMP(" + dateColumn + ", 'Mon FMDD YYYY HH12:MIAM') >= ?::timestamp " +
                     "  AND TO_TIMESTAMP(" + dateColumn + ", 'Mon FMDD YYYY HH12:MIAM') < ?::timestamp";

        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, params.fromDate);
            ps.setString(2, params.toDate);

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
        jsonRoot.setSpecificType(MbJSON.OBJECT);

        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", true);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", "Data retrieved successfully");
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "statusCode", 200);

        MbElement dataArray = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "data", null);
        dataArray.setSpecificType(MbJSON.ARRAY);

        populateResultSetRows(rs, dataArray);

        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "responseCode", "SUCCESS");
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "status", "00");
    }

    private void populateResultSetRows(ResultSet rs, MbElement dataArray) throws Exception {
        ResultSetMetaData rsmd = rs.getMetaData();
        int colCount = rsmd.getColumnCount();
        SimpleDateFormat tsFormat = new SimpleDateFormat(TIMESTAMP_FORMAT);

        while (rs.next()) {
            MbElement item = dataArray.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
            item.setSpecificType(MbJSON.OBJECT);
            mapRowColumns(rs, rsmd, colCount, item, tsFormat);
        }
    }

    private void mapRowColumns(ResultSet rs, ResultSetMetaData rsmd, int colCount, MbElement item, SimpleDateFormat tsFormat) throws Exception {
        for (int i = 1; i <= colCount; i++) {
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