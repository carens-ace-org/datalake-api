package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;

import java.sql.*;
import java.text.SimpleDateFormat;

public class GetDaDimCustomersV2Processor extends MbJavaComputeNode {

    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.S";
    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";
    
    private static final String CUSTOMERS_V2_SQL = 
        "SELECT * " +
        "FROM public.\"DA_Dim_Customer\" c " +
        "JOIN public.\"DA_Dim_Scheme\" s ON c.\"COUNTRY\" = s.\"COUNTRY\" " +
        "AND c.\"SCHEME_CODE\" = s.\"SCHEME_CODE\" " +
        "AND c.\"COMPANY_CODE\" = s.\"COMPANY_CODE\" " +
        "AND c.\"BRANCH_CODE\" = s.\"BRANCH_CODE\" " +
        "WHERE TO_TIMESTAMP(c.\"_date_changed\", 'Mon DD YYYY HH12:MIAM') >= CURRENT_DATE - INTERVAL '1 day' " +
        "OR TO_TIMESTAMP(c.\"_date_changed\", 'Mon DD YYYY HH12:MIAM') < CURRENT_DATE " +
        "LIMIT ? " +
        "OFFSET ?";

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
            String pageNumberStr = Utils.getParam(inAssembly, "pageNumber", "PageNumber");
            String pageSizeStr = Utils.getParam(inAssembly, "pageSize", "PageSize");

            int pageNumber = (pageNumberStr != null && !pageNumberStr.trim().isEmpty()) ? Integer.parseInt(pageNumberStr.trim()) : 0;
            int pageSize = (pageSizeStr != null && !pageSizeStr.trim().isEmpty()) ? Integer.parseInt(pageSizeStr.trim()) : 10;

            if (pageNumber < 0 || pageSize <= 0) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "pageNumber must be >= 0 and pageSize must be > 0.");
                alternate.propagate(alternateAssembly);
                return;
            }

            int offset = pageNumber * pageSize;

            executeQueryAndBuildResponse(outMessage, pageSize, offset);
            out.propagate(outAssembly);

        } catch (NumberFormatException e) {
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "Invalid query parameter format. pageNumber and pageSize must be integers.");
            alternate.propagate(alternateAssembly);
        } catch (Exception e) {
            System.err.println("Database Error: " + e.getMessage());
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: " + e.getMessage());
            alternate.propagate(alternateAssembly);
        }
    }

    private void executeQueryAndBuildResponse(MbMessage outMessage, int pageSize, int offset) throws Exception {
        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(CUSTOMERS_V2_SQL)) {

            ps.setInt(1, pageSize);
            ps.setInt(2, offset);

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