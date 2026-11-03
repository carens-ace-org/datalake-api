package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;

import java.sql.*;
import java.text.SimpleDateFormat;

public class GetIsfFactPolicyCustomersV2Processor extends MbJavaComputeNode {

    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.S";
    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";
    
    private static final String POLICY_CUSTOMERS_V2_SQL = 
        "SELECT p.\"PRODUCT_NAME\", m.* " +
        "FROM public.\"ISF_Fact_Policy\" m " +
        "JOIN public.\"ISF_Dim_Product\" p ON m.\"PRODUCT_CODE\" = p.\"PRODUCT_CODE\" " +
        "WHERE %s::date >= ?::date " +
        "  AND %s::date < ?::date " +
        "  AND (? IS NULL OR p.\"PRODUCT_CODE\" = ?) " +
        "ORDER BY m.\"LIFE_ASSURED_REF_NO\" " +
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

            String pageStr = Utils.getParam(inAssembly, "page", "Page");
            String sizeStr = Utils.getParam(inAssembly, "size", "Size");

            if (isEmpty(fromDate) || isEmpty(toDate) || isEmpty(status)) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Missing required query parameters: fromDate, toDate, and status are mandatory.");
                alternate.propagate(alternateAssembly);
                return;
            }

            String upperStatus = status.trim().toUpperCase();
            if (!"NEW".equals(upperStatus) && !"UPDATED".equals(upperStatus)) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Invalid status parameter. Must be either 'NEW' or 'UPDATED'.");
                alternate.propagate(alternateAssembly);
                return;
            }

            int page = 0;
            int size = 400;

            try {
                if (!isEmpty(pageStr)) {
                    page = Math.max(0, Integer.parseInt(pageStr.trim()));
                }
                if (!isEmpty(sizeStr)) {
                    size = Math.max(1, Integer.parseInt(sizeStr.trim()));
                }
            } catch (NumberFormatException e) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Invalid page or size parameter. Must be an integer.");
                alternate.propagate(alternateAssembly);
                return;
            }

            int offset = page * size;
            String targetCol = "NEW".equals(upperStatus) ? "m.\"LOAD_DATE\"" : "m.\"UPDATE_DATE\"";

            executeQueryAndBuildResponse(outMessage, targetCol, fromDate.trim(), toDate.trim(), productCode, size, offset);
            out.propagate(outAssembly);

        } catch (Exception e) {
            System.err.println("Database Error: " + e.getMessage());
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: There was a problem establishing a connection to the given database URL.");
            alternate.propagate(alternateAssembly);
        }
    }

    private boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    private void executeQueryAndBuildResponse(MbMessage outMessage, String targetCol, String fromDate, String toDate, String productCode, int size, int offset) throws Exception {
        String sql = String.format(POLICY_CUSTOMERS_V2_SQL, targetCol, targetCol);

        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            int paramIndex = 1;
            ps.setString(paramIndex++, fromDate);
            ps.setString(paramIndex++, toDate);

            if (!isEmpty(productCode)) {
                ps.setString(paramIndex++, productCode.trim());
                ps.setString(paramIndex++, productCode.trim());
            } else {
                ps.setNull(paramIndex++, Types.VARCHAR);
                ps.setNull(paramIndex++, Types.VARCHAR);
            }

            ps.setInt(paramIndex++, size);
            ps.setInt(paramIndex++, offset);

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
            MbElement itemObj = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
            itemObj.setSpecificType(MbJSON.OBJECT);
            mapRowColumns(rs, rsmd, columnCount, itemObj, tsFormat);
        }
    }

    private void mapRowColumns(ResultSet rs, ResultSetMetaData rsmd, int columnCount, MbElement itemObj, SimpleDateFormat tsFormat) throws Exception {
        for (int i = 1; i <= columnCount; i++) {
            String colName = rsmd.getColumnLabel(i);
            Object colValue = rs.getObject(i);

            if (colValue == null) {
                continue;
            }

            if (colValue instanceof Number || colValue instanceof Boolean) {
                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue);
            } else if (colValue instanceof Timestamp) {
                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, tsFormat.format((Timestamp) colValue));
            } else {
                itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue.toString());
            }
        }
    }
}