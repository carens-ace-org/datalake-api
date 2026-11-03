package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import java.sql.*;

public class V2PaginatedEnvelopeProcessor extends MbJavaComputeNode {

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
            int pageNumber = getIntParam(inAssembly, "pageNumber", 0);
            int pageSize = getIntParam(inAssembly, "pageSize", 10);
            int offset = pageNumber * pageSize;

            String path = getRequestPath(inAssembly);
            String sql = buildSqlForPath(path);

            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, pageSize);
                ps.setInt(2, offset);

                try (ResultSet rs = ps.executeQuery()) {
                    MbElement root = outMessage.getRootElement();
                    root.createElementAsLastChild("HTTPRequestHeader")
                        .createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

                    MbElement json = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
                    json.setSpecificType(MbJSON.OBJECT);

                    json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", true);
                    json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", "Data retrieved successfully");
                    json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "statusCode", 200);

                    MbElement dataArray = json.createElementAsLastChild(MbElement.TYPE_NAME, "data", null);
                    dataArray.setSpecificType(MbJSON.ARRAY);

                    ResultSetMetaData rsmd = rs.getMetaData();
                    int colCount = rsmd.getColumnCount();

                    while (rs.next()) {
                        MbElement item = dataArray.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
                        item.setSpecificType(MbJSON.OBJECT);
                        for (int i = 1; i <= colCount; i++) {
                            Object val = rs.getObject(i);
                            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, rsmd.getColumnLabel(i), val != null ? val.toString() : null);
                        }
                    }

                    json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "responseCode", "SUCCESS");
                    json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "status", "00");

                    out.propagate(outAssembly);
                }
            }

        } catch (Exception e) {
            sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: " + e.getMessage());
            alternate.propagate(alternateAssembly);
        }
    }

    private String buildSqlForPath(String path) {
        if (path.contains("/v2/isf/dim/customers")) {
            return "SELECT c.*, p.POLICY_NUMBER, pr.PRODUCT_NAME FROM `ISF_Dim_Customer` c " +
                   "LEFT JOIN `ISF_Fact_Policy` p ON CONCAT(p.PAYER_REF_NO) = CONCAT(c.CUSTOMER_REFERENCE) " +
                   "LEFT JOIN `ISF_Dim_Product` pr ON CONCAT(p.PRODUCT_CODE) = CONCAT(pr.PRODUCT_CODE) AND CONCAT(p.COUNTRY) = CONCAT(pr.COUNTRY) " +
                   "WHERE DATE(c.LOAD_DATE) = CURDATE() - INTERVAL 1 DAY OR DATE(c.UPDATE_DATE) = CURDATE() - INTERVAL 1 DAY LIMIT ? OFFSET ?";
        }
        if (path.contains("/v2/isf/dim/beneficiaries")) {
            return "SELECT b.*, p.PAYER_REF_NO, pr.PRODUCT_NAME FROM `ISF_Dim_Beneficiary` b " +
                   "JOIN `ISF_Fact_Policy` p ON CONCAT(b.POLICY_NUMBER) = CONCAT(p.POLICY_NUMBER) " +
                   "JOIN `ISF_Dim_Product` pr ON CONCAT(p.PRODUCT_CODE) = CONCAT(pr.PRODUCT_CODE) AND CONCAT(p.COUNTRY) = CONCAT(pr.COUNTRY) " +
                   "WHERE CONCAT(p.PAYER_REF_NO) <> CONCAT(p.LIFE_ASSURED_REF_NO) " +
                   "AND (DATE(b.LOAD_DATE) = CURDATE() - INTERVAL 1 DAY OR DATE(b.UPDATE_DATE) = CURDATE() - INTERVAL 1 DAY) LIMIT ? OFFSET ?";
        }
        if (path.contains("/v2/da/dim/customers")) {
            return "SELECT c.*, s.SCHEME_NAME FROM `DA_Dim_Customer` c " +
                   "JOIN `DA_Dim_Scheme` s ON CONCAT(c.COUNTRY) = CONCAT(s.COUNTRY) AND CONCAT(c.SCHEME_CODE) = CONCAT(s.SCHEME_CODE) " +
                   "AND CONCAT(c.COMPANY_CODE) = CONCAT(s.COMPANY_CODE) AND CONCAT(c.BRANCH_CODE) = CONCAT(s.BRANCH_CODE) " +
                   "WHERE STR_TO_DATE(c.`_date_changed`, '%b %d %Y %h:%i%p') >= CURDATE() - INTERVAL 1 DAY OR STR_TO_DATE(c.`_date_changed`, '%b %d %Y %h:%i%p') < CURDATE() LIMIT ? OFFSET ?";
        }
        // 5.24 DA Beneficiaries (MySQL Subquery for DISTINCT ON)
        return "SELECT * FROM (SELECT b.*, c.CUSTOMER_ID, c.MEMBER_NUMBER, s.SCHEME_NAME, ROW_NUMBER() OVER (PARTITION BY c.CUSTOMER_ID ORDER BY b._date_loaded DESC) as rn " +
               "FROM `DA_Dim_Beneficiaries` b " +
               "LEFT JOIN `DA_Dim_Customer` c ON CONCAT(b.MEMBER_NO) = CONCAT(c.MEMBER_NUMBER) AND CONCAT(b.COUNTRY) = CONCAT(c.COUNTRY) " +
               "LEFT JOIN `DA_Dim_Scheme` s ON CONCAT(c.COUNTRY) = CONCAT(s.COUNTRY) AND CONCAT(c.SCHEME_CODE) = CONCAT(s.SCHEME_CODE) AND CONCAT(c.COMPANY_CODE) = CONCAT(s.COMPANY_CODE) AND CONCAT(c.BRANCH_CODE) = CONCAT(s.BRANCH_CODE) " +
               "WHERE DATE(b._date_loaded) = CURDATE() - INTERVAL 1 DAY OR DATE(b._date_changed) = CURDATE() - INTERVAL 1 DAY) t WHERE t.rn = 1 LIMIT ? OFFSET ?";
    }

    private void sendErrorResponse(MbMessage altMsg, MbMessage altEnv, int code, String msg) throws MbException {
        MbElement dest = altEnv.getRootElement().createElementAsLastChild(MbElement.TYPE_NAME, "Destination", null)
                                               .createElementAsLastChild(MbElement.TYPE_NAME, "HTTP", null);
        dest.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "ReplyStatusCode", code);

        MbElement root = altMsg.getRootElement();
        root.createElementAsLastChild("HTTPRequestHeader").createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        MbElement json = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", false);
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", msg);
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "statusCode", code);
    }

    private int getIntParam(MbMessageAssembly assembly, String key, int defVal) {
        try {
            MbElement el = assembly.getLocalEnvironment().getRootElement().getFirstElementByPath("REST/Input/Parameters/" + key);
            if (el == null) el = assembly.getLocalEnvironment().getRootElement().getFirstElementByPath("HTTP/Input/QueryString/" + key);
            if (el != null && el.getValueAsString() != null) return Integer.parseInt(el.getValueAsString().trim());
        } catch (Exception ignored) {}
        return defVal;
    }

    private String getRequestPath(MbMessageAssembly assembly) {
        try {
            MbElement path = assembly.getLocalEnvironment().getRootElement().getFirstElementByPath("HTTP/Input/RequestURL");
            if (path != null) return path.getValueAsString();
        } catch (Exception ignored) {}
        return "";
    }
}