package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;
import java.sql.*;
import java.text.SimpleDateFormat;

public class GetActisureDimPolicyNotesProcessor extends MbJavaComputeNode {

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
            String policyIdStr = getPathParam(inAssembly, "policyId");
            Long policyId = parseAndValidatePolicyId(policyIdStr, alternateMessage, outLocalEnv);
            if (policyId == null) {
                alternate.propagate(alternateAssembly);
                return;
            }

            executeAndBuildResponse(outMessage, policyId);
            out.propagate(outAssembly);

        } catch (Exception e) {
        	System.err.println("Database Error: " + e.getMessage());
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: There was a problem establishing a connection to the given database URL.");
            alternate.propagate(alternateAssembly);
        
        }
    }

    private Long parseAndValidatePolicyId(String policyIdStr, MbMessage altMsg, MbMessage altEnv) throws MbException {
        if (policyIdStr == null || policyIdStr.trim().isEmpty()) {
            Utils.sendErrorResponse(altMsg, altEnv, 400, "Missing required path parameter: policyId is mandatory.");
            return null;
        }
        try {
            return Long.parseLong(policyIdStr.trim());
        } catch (NumberFormatException nfe) {
            Utils.sendErrorResponse(altMsg, altEnv, 400, "Invalid policyId parameter: must be an integer.");
            return null;
        }
    }

    private void executeAndBuildResponse(MbMessage outMessage, long policyId) throws Exception {
        String sql = "SELECT * FROM public.\"Actisure_Dim_Policy_Notes\" WHERE \"PolicyId\" = ?";

        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setLong(1, policyId);

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

    private String getPathParam(MbMessageAssembly assembly, String key) {
        try {
            MbElement root = assembly.getLocalEnvironment().getRootElement();
            MbElement el = root.getFirstElementByPath("REST/Input/Parameters/" + key);
            if (el != null && el.getValueAsString() != null) return el.getValueAsString().trim();

            el = root.getFirstElementByPath("HTTP/Input/Path/" + key);
            if (el != null && el.getValueAsString() != null) return el.getValueAsString().trim();
        } catch (Exception ignored) {}
        return null;
    }
}