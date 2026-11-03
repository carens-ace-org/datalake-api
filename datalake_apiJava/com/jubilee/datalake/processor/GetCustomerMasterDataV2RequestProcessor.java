package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class GetCustomerMasterDataV2RequestProcessor extends MbJavaComputeNode {

    private static final String BASE_CUSTOMER_SQL = """
        SELECT 
            "CUSTOMER_ID" AS "beneficiaryId", 
            "CUSTOMER_NAME" AS "principalName", 
            "CUSTOMER_NAME" AS "beneficiaryName", 
            "NATIONAL_ID" AS "nationalIdNumber", 
            "TAX_NUMBER" AS "taxIdNumber", 
            "CUSTOMER_GENDER" AS "customerGender", 
            "EMAIL_ADDRESS" AS "emailAddress", 
            "MOBILE_NUMBER" AS "mobileNumber", 
            "DOB" AS "dob", 
            "COUNTRY" AS "country", 
            "IS_IMPORTED" AS "isImported", 
            "ACTISURE" AS "actisure", 
            "DA" AS "da", 
            "ISF" AS "isf", 
            "JAML" AS "jaml", 
            "CUSTOMER_SYS_ID_ACTISURE" AS "customerSysIdActisure", 
            "CUSTOMER_SYS_ID_DA" AS "customerSysIdDa", 
            "CUSTOMER_SYS_ID_ISF" AS "customerSysIdIsf", 
            "CUSTOMER_SYS_ID_JAML" AS "customerSysIdJaml", 
            "ACTISURE_CUSTOMER_TYPE" AS "actisureCustomerType", 
            "DA_CUSTOMER_TYPE" AS "daCustomerType", 
            "ISF_CUSTOMER_TYPE" AS "isfCustomerType", 
            "ACT_DATA_PROCESSING_CONSENT" AS "actDataProcessingConsent", 
            "ACT_MARKETING_CONSENT" AS "actMarketingConsent", 
            "ACT_MEDICAL_CONSENT" AS "actMedicalConsent", 
            "DA_DATA_PROCESSING_CONSENT" AS "daDataProcessingConsent", 
            "DA_MARKETING_CONSENT" AS "daMarketingConsent", 
            "ISF_DATA_PROCESSING_CONSENT" AS "isfDataProcessingConsent", 
            "ISF_MARKETING_CONSENT" AS "isfMarketingConsent", 
            "JAML_DATA_PROCESSING_CONSENT" AS "jamlDataProcessingConsent", 
            "JAML_MARKETING_CONSENT" AS "jamlMarketingConsent", 
            "HashKey" AS "hashKey", 
            "LOAD_DATE" AS "loadDate", 
            "OLD_CUSTOMER_ID" AS "oldCustomerId" 
        FROM "DIM_JHL_CUSTOMER"
        WHERE 1=1
        """;

    private static final String BASE_BENEFICIARY_SQL = """
        SELECT 
            "BENEFICIARY_ID" AS "beneficiaryId", 
            "BENEFICIARY_NAME" AS "principalName", 
            "BENEFICIARY_NAME" AS "beneficiaryName", 
            "NATIONAL_ID" AS "nationalIdNumber", 
            "TAX_NUMBER" AS "taxIdNumber", 
            "GENDER" AS "customerGender", 
            "EMAIL_ADDRESS" AS "emailAddress", 
            "MOBILE_NUMBER" AS "mobileNumber", 
            "DOB" AS "dob", 
            "COUNTRY" AS "country", 
            "IS_IMPORTED" AS "isImported", 
            "ACTISURE" AS "actisure", 
            "DA" AS "da", 
            "ISF" AS "isf", 
            "BENEFICIARY_SYS_ID_ACTISURE" AS "customerSysIdActisure", 
            "BENEFICIARY_SYS_ID_DA" AS "customerSysIdDa", 
            "BENEFICIARY_SYS_ID_ISF" AS "customerSysIdIsf", 
            "HashKey" AS "hashKey", 
            "LOAD_DATE" AS "loadDate", 
            "OLD_BENEFICIARY_ID" AS "oldCustomerId" 
        FROM "jhl_fact_dim"."DIM_JHL_BENEFICIARY"
        WHERE 1=1
        """;

    // Execution Context per node execution
    private MbMessageAssembly inAssembly;
    private MbMessage outMessage;
    private MbMessage alternateMessage;
    private MbMessage outLocalEnv;
    private MbOutputTerminal out;
    private MbOutputTerminal alternate;
    private MbMessageAssembly outAssembly;
    private MbMessageAssembly alternateAssembly;

    @Override
    public void evaluate(MbMessageAssembly assembly) throws MbException {
        this.inAssembly = assembly;
        this.out = getOutputTerminal("out");
        this.alternate = getOutputTerminal("alternate");

        this.outMessage = new MbMessage();
        this.alternateMessage = new MbMessage();
        this.outLocalEnv = new MbMessage(inAssembly.getLocalEnvironment());

        this.outAssembly = new MbMessageAssembly(inAssembly, outLocalEnv, inAssembly.getExceptionList(), outMessage);
        this.alternateAssembly = new MbMessageAssembly(inAssembly, outLocalEnv, inAssembly.getExceptionList(), alternateMessage);

        try {
            processRequest();
        } catch (Exception e) {
            sendErrorResponse(500, "Internal Server Error: " + e.getMessage());
            alternate.propagate(alternateAssembly);
        }
    }

    public void processRequest() throws Exception {
        // 1. Extract Identifiers
        String nationalId = getParam("NationalID", "NationalId", "nationalIdNumber");
        String taxId = getParam("TaxpayerID", "TaxId", "taxIdNumber");
        String entityId = getParam("EntityID", "EntityId", "entityId");

        boolean hasNationalId = (nationalId != null && !nationalId.trim().isEmpty());
        boolean hasTaxId = (taxId != null && !taxId.trim().isEmpty());
        boolean hasEntityId = (entityId != null && !entityId.trim().isEmpty());

        // 2. Validate parameters
        if (!hasNationalId && !hasTaxId && !hasEntityId) {
            sendErrorResponse(400, "At least one search parameter (NationalID, TaxpayerID, or EntityID) must be provided.");
            alternate.propagate(alternateAssembly);
            return;
        }

        // 3. Connect via Dynamic Policy Pool
        Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);

        // Construct Root Response
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

        MbElement detailsArray = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "details", null);
        detailsArray.setSpecificType(MbJSON.ARRAY);

        // 4. Primary Query: DIM_JHL_CUSTOMER
        String sqlCustomer = BASE_CUSTOMER_SQL
                + (hasNationalId ? "  AND \"NATIONAL_ID\" = ?\n" : "")
                + (hasTaxId ? "  AND \"TAX_NUMBER\" = ?\n" : "")
                + (hasEntityId ? "  AND \"CUSTOMER_SYS_ID_ACTISURE\" = ?\n" : "");

        List<String> customerParams = new ArrayList<>();
        if (hasNationalId) customerParams.add(nationalId.trim());
        if (hasTaxId) customerParams.add(taxId.trim());
        if (hasEntityId) customerParams.add(entityId.trim());

        boolean recordFound = queryAndPopulateDetails(conn, sqlCustomer, customerParams, detailsArray);

        // 5. Fallback Query: DIM_JHL_BENEFICIARY (if no customer matches found)
        if (!recordFound) {
            String sqlBeneficiary = BASE_BENEFICIARY_SQL
                    + (hasNationalId ? "  AND \"NATIONAL_ID\" = ?\n" : "")
                    + (hasTaxId ? "  AND \"TAX_NUMBER\" = ?\n" : "")
                    + (hasEntityId ? "  AND \"BENEFICIARY_SYS_ID_ACTISURE\" = ?\n" : "");

            List<String> beneficiaryParams = new ArrayList<>();
            if (hasNationalId) beneficiaryParams.add(nationalId.trim());
            if (hasTaxId) beneficiaryParams.add(taxId.trim());
            if (hasEntityId) beneficiaryParams.add(entityId.trim());

            recordFound = queryAndPopulateDetails(conn, sqlBeneficiary, beneficiaryParams, detailsArray);
        }

        // 6. Propagate Success or 404 Response
        if (recordFound) {
            out.propagate(outAssembly);
        } else {
            sendErrorResponse(404, "No customer record found matching the provided search criteria.");
            alternate.propagate(alternateAssembly);
        }
    }

    private boolean queryAndPopulateDetails(Connection conn, String sql, List<String> params, MbElement detailsArray) throws Exception {
        boolean hasResults = false;
        SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                ps.setString(i + 1, params.get(i));
            }

            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnCount = rsmd.getColumnCount();

                while (rs.next()) {
                    hasResults = true;
                    MbElement itemObj = detailsArray.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
                    itemObj.setSpecificType(MbJSON.OBJECT);

                    for (int i = 1; i <= columnCount; i++) {
                        String colName = rsmd.getColumnLabel(i);
                        Object colValue = rs.getObject(i);

                        if (colValue == null) {
                            continue;
                        }

                        if (colValue instanceof Number) {
                            itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, colName, colValue);
                        } else if (colValue instanceof Boolean) {
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
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        return hasResults;
    }

    private String getParam(String... keys) {
        try {
            for (String key : keys) {
                MbElement jsonBody = inAssembly.getMessage().getRootElement().getFirstElementByPath("JSON/Data/" + key);
                if (jsonBody != null && jsonBody.getValueAsString() != null) {
                    return jsonBody.getValueAsString().trim();
                }

                MbElement root = inAssembly.getLocalEnvironment().getRootElement();
                MbElement el = root.getFirstElementByPath("REST/Input/Parameters/" + key);
                if (el != null && el.getValueAsString() != null) return el.getValueAsString().trim();

                el = root.getFirstElementByPath("HTTP/Input/QueryString/" + key);
                if (el != null && el.getValueAsString() != null) return el.getValueAsString().trim();
            }
        } catch (Exception ignored) {}
        return null;
    }

    private void sendErrorResponse(int code, String msg) throws MbException {
        MbElement localEnvRoot = outLocalEnv.getRootElement();
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

        MbElement root = alternateMessage.getRootElement();
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
}