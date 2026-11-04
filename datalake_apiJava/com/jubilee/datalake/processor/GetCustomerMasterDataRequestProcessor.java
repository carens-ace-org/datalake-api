package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class GetCustomerMasterDataRequestProcessor extends MbJavaComputeNode {

    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";
    
    private static final String BASE_SQL = 
        "SELECT " +
        "    \"CUSTOMER_ID\" AS \"beneficiaryId\", " +
        "    \"CUSTOMER_NAME\" AS \"principalName\", " +
        "    \"CUSTOMER_NAME\" AS \"beneficiaryName\", " +
        "    \"NATIONAL_ID\" AS \"nationalIdNumber\", " +
        "    \"TAX_NUMBER\" AS \"taxIdNumber\", " +
        "    \"CUSTOMER_GENDER\" AS \"customerGender\", " +
        "    \"EMAIL_ADDRESS\" AS \"emailAddress\", " +
        "    \"MOBILE_NUMBER\" AS \"mobileNumber\", " +
        "    \"DOB\" AS \"dob\", " +
        "    \"COUNTRY\" AS \"country\", " +
        "    \"IS_IMPORTED\" AS \"isImported\", " +
        "    \"ACTISURE\" AS \"actisure\", " +
        "    \"DA\" AS \"da\", " +
        "    \"ISF\" AS \"isf\", " +
        "    \"JAML\" AS \"jaml\", " +
        "    \"CUSTOMER_SYS_ID_ACTISURE\" AS \"customerSysIdActisure\", " +
        "    \"CUSTOMER_SYS_ID_DA\" AS \"customerSysIdDa\", " +
        "    \"CUSTOMER_SYS_ID_ISF\" AS \"customerSysIdIsf\", " +
        "    \"CUSTOMER_SYS_ID_JAML\" AS \"customerSysIdJaml\", " +
        "    \"ACTISURE_CUSTOMER_TYPE\" AS \"actisureCustomerType\", " +
        "    \"DA_CUSTOMER_TYPE\" AS \"daCustomerType\", " +
        "    \"ISF_CUSTOMER_TYPE\" AS \"isfCustomerType\", " +
        "    \"ACT_DATA_PROCESSING_CONSENT\" AS \"actDataProcessingConsent\", " +
        "    \"ACT_MARKETING_CONSENT\" AS \"actMarketingConsent\", " +
        "    \"ACT_MEDICAL_CONSENT\" AS \"actMedicalConsent\", " +
        "    \"DA_DATA_PROCESSING_CONSENT\" AS \"daDataProcessingConsent\", " +
        "    \"DA_MARKETING_CONSENT\" AS \"daMarketingConsent\", " +
        "    \"ISF_DATA_PROCESSING_CONSENT\" AS \"isfDataProcessingConsent\", " +
        "    \"ISF_MARKETING_CONSENT\" AS \"isfMarketingConsent\", " +
        "    \"JAML_DATA_PROCESSING_CONSENT\" AS \"jamlDataProcessingConsent\", " +
        "    \"JAML_MARKETING_CONSENT\" AS \"jamlMarketingConsent\", " +
        "    \"HashKey\" AS \"hashKey\", " +
        "    \"LOAD_DATE\" AS \"loadDate\", " +
        "    \"OLD_CUSTOMER_ID\" AS \"oldCustomerId\" " +
        "FROM \"DIM_JHL_CUSTOMER\" " +
        "WHERE 1=1";

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
            SearchCriteria criteria = extractSearchCriteria(inAssembly);
            if (!criteria.isValid()) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, 
                    "Missing search criteria: At least one search identifier (nationalIdNumber, taxIdNumber, entityId) must be provided.");
                alternate.propagate(alternateAssembly);
                return;
            }

            List<RecordData> records = executeQuery(criteria);
            if (records.isEmpty()) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 404, 
                    "No customer record found matching the provided search criteria.");
                alternate.propagate(alternateAssembly);
                return;
            }

            buildJsonPayload(outMessage, records);
            out.propagate(outAssembly);

        } catch (SQLException e) {
            String reason = (e.getMessage() != null) ? e.getMessage() : "Unable to reach database service.";
            System.err.println("Database Error: " + reason);
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, "DB Connection Failed: " + reason);
            alternate.propagate(alternateAssembly);
        } catch (Exception e) {
            String reason = (e.getMessage() != null) ? e.getMessage() : e.getClass().getSimpleName();
            System.err.println("Internal Error: " + reason);
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: " + reason);
            alternate.propagate(alternateAssembly);
        }
    }

    private SearchCriteria extractSearchCriteria(MbMessageAssembly inAssembly) {
        String nationalId = Utils.getParam(inAssembly, "nationalIdNumber", "NationalIdNumber");
        String taxId = Utils.getParam(inAssembly, "taxIdNumber", "TaxIdNumber");
        String entityId = Utils.getParam(inAssembly, "entityId", "EntityID", "entityid");
        return new SearchCriteria(nationalId, taxId, entityId);
    }

    private List<RecordData> executeQuery(SearchCriteria criteria) throws Exception {
        QueryContext ctx = buildQueryContext(criteria);
        List<RecordData> results = new ArrayList<>();

        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(ctx.sql)) {

            bindParameters(ps, ctx.params);

            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                while (rs.next()) {
                    results.add(extractRecordData(rs, rsmd));
                }
            }
        }
        return results;
    }

    private void bindParameters(PreparedStatement ps, List<String> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            ps.setString(i + 1, params.get(i));
        }
    }

    private RecordData extractRecordData(ResultSet rs, ResultSetMetaData rsmd) throws SQLException {
        RecordData record = new RecordData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String colName = rsmd.getColumnLabel(i);
            Object colValue = rs.getObject(i);
            if (colValue != null) {
                record.columns.put(colName, colValue);
            }
        }
        return record;
    }

    private QueryContext buildQueryContext(SearchCriteria sc) {
        StringBuilder sql = new StringBuilder(BASE_SQL);
        List<String> params = new ArrayList<>();
        appendCondition(sc.nationalId, "NATIONAL_ID", sql, params);
        appendCondition(sc.taxId, "TAX_NUMBER", sql, params);
        appendCondition(sc.entityId, "CUSTOMER_SYS_ID_ACTISURE", sql, params);
        return new QueryContext(sql.toString(), params);
    }

    private void appendCondition(String val, String column, StringBuilder sql, List<String> params) {
        if (val != null && !val.trim().isEmpty()) {
            sql.append(" AND \"").append(column).append("\" = ?\n");
            params.add(val.trim());
        }
    }

    private void buildJsonPayload(MbMessage outMessage, List<RecordData> records) throws Exception {
        MbElement root = outMessage.getRootElement();
        MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        jsonRoot.setSpecificType(MbJSON.OBJECT);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", true);

        MbElement statusObj = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "status", null);
        statusObj.setSpecificType(MbJSON.OBJECT);
        statusObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "code", 200);
        statusObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", "OK");

        MbElement detailsArray = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "details", null);
        detailsArray.setSpecificType(MbJSON.ARRAY);

        for (RecordData record : records) {
            MbElement itemObj = detailsArray.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
            itemObj.setSpecificType(MbJSON.OBJECT);
            for (Map.Entry<String, Object> entry : record.columns.entrySet()) {
                appendColumnElement(itemObj, entry.getKey(), entry.getValue(), isoFormat);
            }
        }
    }

    private void appendColumnElement(MbElement itemObj, String colName, Object colValue, SimpleDateFormat isoFormat) throws MbException {
        if (colValue instanceof Number || colValue instanceof Boolean) {
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue);
        } else if (colValue instanceof Timestamp) {
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, isoFormat.format((Timestamp) colValue));
        } else if (colValue instanceof java.sql.Date) {
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, isoFormat.format(new Date(((java.sql.Date) colValue).getTime())));
        } else {
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue.toString());
        }
    }

    private static class SearchCriteria {
        final String nationalId;
        final String taxId;
        final String entityId;

        SearchCriteria(String nationalId, String taxId, String entityId) {
            this.nationalId = nationalId;
            this.taxId = taxId;
            this.entityId = entityId;
        }

        boolean isValid() {
            return isPresent(nationalId) || isPresent(taxId) || isPresent(entityId);
        }

        private boolean isPresent(String s) {
            return s != null && !s.trim().isEmpty();
        }
    }

    private static class QueryContext {
        final String sql;
        final List<String> params;

        QueryContext(String sql, List<String> params) {
            this.sql = sql;
            this.params = params;
        }
    }

    private static class RecordData {
        final Map<String, Object> columns = new LinkedHashMap<>();
    }
}