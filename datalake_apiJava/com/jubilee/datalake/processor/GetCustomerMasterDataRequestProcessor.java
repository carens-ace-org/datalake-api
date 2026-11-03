package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.utils.Utils;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

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
            // Extract from JSON Request Body or Query Parameters
            String nationalId = getBodyOrQueryParam(inAssembly, "nationalIdNumber", "NationalIdNumber");
            String taxId = getBodyOrQueryParam(inAssembly, "taxIdNumber", "TaxIdNumber");
            String entityId = getBodyOrQueryParam(inAssembly, "entityId", "EntityID", "entityid");

            boolean hasNationalId = !isEmpty(nationalId);
            boolean hasTaxId = !isEmpty(taxId);
            boolean hasEntityId = !isEmpty(entityId);

            if (!hasNationalId && !hasTaxId && !hasEntityId) {
                sendErrorResponse(alternateMessage, outLocalEnv, 400, "Missing search criteria: At least one search identifier (nationalIdNumber, taxIdNumber, entityId) must be provided.");
                alternate.propagate(alternateAssembly);
                return;
            }

            QueryContext queryCtx = buildQuery(hasNationalId, hasTaxId, hasEntityId, nationalId, taxId, entityId);
            List<RecordData> records = executeQuery(queryCtx);

            if (!records.isEmpty()) {
                buildJsonPayload(outMessage, records);
                out.propagate(outAssembly);
            } else {
                sendErrorResponse(alternateMessage, outLocalEnv, 404, "No customer record found matching the provided search criteria.");
                alternate.propagate(alternateAssembly);
            }

        } catch (SQLException e) {
            String reason = (e.getMessage() != null) ? e.getMessage() : "Unable to reach database service.";
            System.err.println("Database Error: " + reason);
            sendErrorResponse(alternateMessage, outLocalEnv, 500, "DB Connection Failed: " + reason);
            alternate.propagate(alternateAssembly);
        } catch (Exception e) {
            System.err.println("Internal Error: " + e.getMessage());
            sendErrorResponse(alternateMessage, outLocalEnv, 500, "Internal Server Error: " + e.getMessage());
            alternate.propagate(alternateAssembly);
        }
    }

    private boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    private String getBodyOrQueryParam(MbMessageAssembly inAssembly, String... keys) {
        try {
            // 1. Check incoming JSON Body Payload
            MbElement msgRoot = inAssembly.getMessage().getRootElement();
            for (String key : keys) {
                MbElement jsonEl = msgRoot.getFirstElementByPath("JSON/Data/" + key);
                if (jsonEl == null) {
                    jsonEl = msgRoot.getFirstElementByPath("JSON/" + key);
                }
                if (jsonEl != null && jsonEl.getValueAsString() != null && !jsonEl.getValueAsString().trim().isEmpty()) {
                    return jsonEl.getValueAsString().trim();
                }
            }
        } catch (Exception ignored) {}

        // 2. Fall back to standard Utils parameter extraction (Query/REST params)
        return Utils.getParam(inAssembly, keys);
    }

    private QueryContext buildQuery(boolean hasNationalId, boolean hasTaxId, boolean hasEntityId, String nationalId, String taxId, String entityId) {
        StringBuilder sql = new StringBuilder(BASE_SQL);
        List<String> params = new ArrayList<>();

        if (hasNationalId) {
            sql.append(" AND \"NATIONAL_ID\" = ?\n");
            params.add(nationalId.trim());
        }
        if (hasTaxId) {
            sql.append(" AND \"TAX_NUMBER\" = ?\n");
            params.add(taxId.trim());
        }
        if (hasEntityId) {
            sql.append(" AND \"CUSTOMER_SYS_ID_ACTISURE\" = ?\n");
            params.add(entityId.trim());
        }

        return new QueryContext(sql.toString(), params);
    }

    private List<RecordData> executeQuery(QueryContext ctx) throws Exception {
        List<RecordData> results = new ArrayList<>();
        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(ctx.sql)) {

            for (int i = 0; i < ctx.params.size(); i++) {
                ps.setString(i + 1, ctx.params.get(i));
            }

            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnCount = rsmd.getColumnCount();

                while (rs.next()) {
                    RecordData record = new RecordData();
                    for (int i = 1; i <= columnCount; i++) {
                        String colName = rsmd.getColumnLabel(i);
                        Object colValue = rs.getObject(i);
                        if (colValue != null) {
                            record.columns.put(colName, colValue);
                        }
                    }
                    results.add(record);
                }
            }
        }
        return results;
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

            for (java.util.Map.Entry<String, Object> entry : record.columns.entrySet()) {
                String colName = entry.getKey();
                Object colValue = entry.getValue();

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
        }
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

        MbElement statusObj = json.createElementAsLastChild(MbElement.TYPE_NAME, "status", null);
        statusObj.setSpecificType(MbJSON.OBJECT);
        statusObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "code", code);
        statusObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", msg);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TimeZone.getTimeZone("Africa/Nairobi"));
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "timestamp", sdf.format(new Date()));
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
        final java.util.Map<String, Object> columns = new java.util.LinkedHashMap<>();
    }
}