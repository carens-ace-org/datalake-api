package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class GetCustomerMasterDataV2RequestProcessor extends MbJavaComputeNode {

    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";
    
    private static final String BASE_CUSTOMER_SQL = 
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

    private static final String BASE_BENEFICIARY_SQL = 
        "SELECT " +
        "    \"BENEFICIARY_ID\" AS \"beneficiaryId\", " +
        "    \"BENEFICIARY_NAME\" AS \"principalName\", " +
        "    \"BENEFICIARY_NAME\" AS \"beneficiaryName\", " +
        "    \"NATIONAL_ID\" AS \"nationalIdNumber\", " +
        "    \"TAX_NUMBER\" AS \"taxIdNumber\", " +
        "    \"GENDER\" AS \"customerGender\", " +
        "    \"EMAIL_ADDRESS\" AS \"emailAddress\", " +
        "    \"MOBILE_NUMBER\" AS \"mobileNumber\", " +
        "    \"DOB\" AS \"dob\", " +
        "    \"COUNTRY\" AS \"country\", " +
        "    \"IS_IMPORTED\" AS \"isImported\", " +
        "    \"ACTISURE\" AS \"actisure\", " +
        "    \"DA\" AS \"da\", " +
        "    \"ISF\" AS \"isf\", " +
        "    \"BENEFICIARY_SYS_ID_ACTISURE\" AS \"customerSysIdActisure\", " +
        "    \"BENEFICIARY_SYS_ID_DA\" AS \"customerSysIdDa\", " +
        "    \"BENEFICIARY_SYS_ID_ISF\" AS \"customerSysIdIsf\", " +
        "    \"HashKey\" AS \"hashKey\", " +
        "    \"LOAD_DATE\" AS \"loadDate\", " +
        "    \"OLD_BENEFICIARY_ID\" AS \"oldCustomerId\" " +
        "FROM \"jhl_fact_dim\".\"DIM_JHL_BENEFICIARY\" " +
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
            String nationalId = getBodyOrQueryParam(inAssembly, "NationalID", "NationalId", "nationalIdNumber");
            String taxId = getBodyOrQueryParam(inAssembly, "TaxpayerID", "TaxId", "taxIdNumber");
            String entityId = getBodyOrQueryParam(inAssembly, "EntityID", "EntityId", "entityId");

            boolean hasNationalId = !isEmpty(nationalId);
            boolean hasTaxId = !isEmpty(taxId);
            boolean hasEntityId = !isEmpty(entityId);

            if (!hasNationalId && !hasTaxId && !hasEntityId) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, 400, "At least one search parameter (NationalID, TaxpayerID, or EntityID) must be provided.");
                alternate.propagate(alternateAssembly);
                return;
            }

            initializeSuccessHeaders(outMessage);
            MbElement detailsArray = getDetailsArray(outMessage);

            try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO)) {
                QueryContext customerQuery = buildQuery(BASE_CUSTOMER_SQL, "NATIONAL_ID", "TAX_NUMBER", "CUSTOMER_SYS_ID_ACTISURE", hasNationalId, hasTaxId, hasEntityId, nationalId, taxId, entityId);
                boolean recordFound = queryAndPopulateDetails(conn, customerQuery, detailsArray);

                if (!recordFound) {
                    QueryContext beneficiaryQuery = buildQuery(BASE_BENEFICIARY_SQL, "NATIONAL_ID", "TAX_NUMBER", "BENEFICIARY_SYS_ID_ACTISURE", hasNationalId, hasTaxId, hasEntityId, nationalId, taxId, entityId);
                    recordFound = queryAndPopulateDetails(conn, beneficiaryQuery, detailsArray);
                }

                if (recordFound) {
                    out.propagate(outAssembly);
                } else {
                    Utils.sendErrorResponse(alternateMessage, outLocalEnv, 404, "No customer record found matching the provided search criteria.");
                    alternate.propagate(alternateAssembly);
                }
            }

        } catch (Exception e) {
            System.err.println("Internal Error: " + e.getMessage());
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: " + e.getMessage());
            alternate.propagate(alternateAssembly);
        }
    }

    private boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    private String getBodyOrQueryParam(MbMessageAssembly inAssembly, String... keys) {
        try {
            MbElement msgRoot = inAssembly.getMessage().getRootElement();
            for (String key : keys) {
                MbElement jsonEl = msgRoot.getFirstElementByPath("JSON/Data/" + key);
                if (jsonEl == null) {
                    jsonEl = msgRoot.getFirstElementByPath("JSON/" + key);
                }
                if (jsonEl != null && !isEmpty(jsonEl.getValueAsString())) {
                    return jsonEl.getValueAsString().trim();
                }
            }
        } catch (Exception ignored) {}
        return Utils.getParam(inAssembly, keys);
    }

    private void initializeSuccessHeaders(MbMessage outMessage) throws MbException {
        MbElement root = outMessage.getRootElement();
        MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        jsonRoot.setSpecificType(MbJSON.OBJECT);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", true);

        MbElement statusObj = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "status", null);
        statusObj.setSpecificType(MbJSON.OBJECT);
        statusObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "code", 200);
        statusObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", "OK");
    }

    private MbElement getDetailsArray(MbMessage outMessage) throws MbException {
        MbElement jsonRoot = outMessage.getRootElement().getFirstElementByPath("JSON/Data");
        MbElement detailsArray = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "details", null);
        detailsArray.setSpecificType(MbJSON.ARRAY);
        return detailsArray;
    }

    private QueryContext buildQuery(String baseSql, String natCol, String taxCol, String entityCol, 
                                    boolean hasNat, boolean hasTax, boolean hasEntity, 
                                    String natVal, String taxVal, String entityVal) {
        StringBuilder sql = new StringBuilder(baseSql);
        List<String> params = new ArrayList<>();

        appendParam(sql, params, hasNat, natCol, natVal);
        appendParam(sql, params, hasTax, taxCol, taxVal);
        appendParam(sql, params, hasEntity, entityCol, entityVal);

        return new QueryContext(sql.toString(), params);
    }

    private void appendParam(StringBuilder sql, List<String> params, boolean condition, String col, String val) {
        if (condition) {
            sql.append(" AND \"").append(col).append("\" = ?\n");
            params.add(val.trim());
        }
    }

    private boolean queryAndPopulateDetails(Connection conn, QueryContext ctx, MbElement detailsArray) throws Exception {
        boolean hasResults = false;
        SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        try (PreparedStatement ps = conn.prepareStatement(ctx.sql)) {
            for (int i = 0; i < ctx.params.size(); i++) {
                ps.setString(i + 1, ctx.params.get(i));
            }

            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnCount = rsmd.getColumnCount();

                while (rs.next()) {
                    hasResults = true;
                    MbElement itemObj = detailsArray.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
                    itemObj.setSpecificType(MbJSON.OBJECT);
                    populateRowColumns(rs, rsmd, columnCount, itemObj, isoFormat);
                }
            }
        } catch (SQLException sqle) {
            System.err.println("SQL Execution Warning: " + sqle.getMessage());
        }
        return hasResults;
    }

    private void populateRowColumns(ResultSet rs, ResultSetMetaData rsmd, int columnCount, MbElement itemObj, SimpleDateFormat isoFormat) throws SQLException, MbException {
        for (int i = 1; i <= columnCount; i++) {
            String colName = rsmd.getColumnLabel(i);
            Object colValue = rs.getObject(i);
            if (colValue != null) {
                appendColumnValue(itemObj, colName, colValue, isoFormat);
            }
        }
    }

    private void appendColumnValue(MbElement itemObj, String colName, Object val, SimpleDateFormat isoFormat) throws MbException {
        if (val instanceof Number || val instanceof Boolean) {
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, val);
        } else if (val instanceof Timestamp) {
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, isoFormat.format((Timestamp) val));
        } else if (val instanceof java.sql.Date) {
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, isoFormat.format(new Date(((java.sql.Date) val).getTime())));
        } else {
            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, val.toString());
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
}