package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import java.sql.*;

public class DateRangeStatusCustomerProcessor extends MbJavaComputeNode {

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

            // Validation (400 Bad Request)
            if (fromDate == null || toDate == null || status == null || 
               (!status.equals("NEW") && !status.equals("UPDATED"))) {
                sendErrorResponse(alternateMessage, outLocalEnv, 400, 
                    "fromDate, toDate, and status are required. status must be NEW or UPDATED.");
                alternate.propagate(alternateAssembly);
                return;
            }

            // Determine Target Table Name dynamically from HTTP Request URL path
            String path = getRequestPath(inAssembly);
            String table = resolveTableName(path);

            // Construct SQL dynamically based on status and table schema
            String dateCol = status.equals("NEW") ? 
                (table.contains("DA_") ? "_date_loaded" : (table.contains("ISF_") ? "LOAD_DATE" : "_date_loaded")) :
                (table.contains("DA_") ? "_date_changed" : (table.contains("ISF_") ? "UPDATE_DATE" : "_date_changed"));

            String sql;
            if (table.contains("DA_")) {
                sql = "SELECT * FROM `" + table + "` WHERE STR_TO_DATE(`" + dateCol + "`, '%b %d %Y %h:%i%p') >= ? AND STR_TO_DATE(`" + dateCol + "`, '%b %d %Y %h:%i%p') < ?";
            } else if (table.contains("ISF_")) {
                sql = "SELECT * FROM `" + table + "` WHERE DATE(`" + dateCol + "`) >= ? AND DATE(`" + dateCol + "`) < ?";
            } else {
                sql = "SELECT * FROM `" + table + "` WHERE `" + dateCol + "` >= ? AND `" + dateCol + "` < ?";
            }

            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, fromDate);
                ps.setString(2, toDate);

                try (ResultSet rs = ps.executeQuery()) {
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
                            Object val = rs.getObject(i);
                            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, rsmd.getColumnLabel(i), val != null ? val.toString() : null);
                        }
                    }
                    out.propagate(outAssembly);
                }
            }

        } catch (Exception e) {
        	
        	System.err.println("Internal Server Error: " + e.getMessage()); 
            sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: " + e.getMessage());
            
            alternate.propagate(alternateAssembly);
        }
    }

    private String resolveTableName(String path) {
        if (path.contains("/isf/dim/customers")) return "ISF_Dim_Customer";
        if (path.contains("/da/dim/customers")) return "DA_Dim_Customer";
        if (path.contains("/actisure/factpolicy/customers")) return "Actisure_Fact_Policy";
        if (path.contains("/isf/factpolicy/customers")) return "ISF_Fact_Policy";
        if (path.contains("/actisure/dimbeneficiary/customers")) return "Actisure_Dim_Beneficiary";
        if (path.contains("/actisure/dimscheme/customers")) return "Actisure_Dim_Scheme";
        if (path.contains("/actisure/dimschemecategory/customers")) return "Actisure_Dim_Scheme_Category";
        return "ISF_Dim_Customer";
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
            if (el != null && el.getValueAsString() != null) return el.getValueAsString().trim();
            el = assembly.getLocalEnvironment().getRootElement().getFirstElementByPath("HTTP/Input/QueryString/" + key);
            if (el != null && el.getValueAsString() != null) return el.getValueAsString().trim();
        } catch (Exception ignored) {}
        return null;
    }

    private String getRequestPath(MbMessageAssembly assembly) {
        try {
            MbElement path = assembly.getLocalEnvironment().getRootElement().getFirstElementByPath("HTTP/Input/RequestURL");
            if (path != null) return path.getValueAsString();
        } catch (Exception ignored) {}
        return "";
    }
}