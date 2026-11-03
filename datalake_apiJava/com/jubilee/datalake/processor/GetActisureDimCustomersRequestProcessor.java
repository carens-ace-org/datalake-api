package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class GetActisureDimCustomersRequestProcessor extends MbJavaComputeNode {

 
    public static class ActisureCustomer {
        private String customerSegmented;
        private String marketingConsent;
        private String dateLoaded;
        private Long entityId;
        private String customerId;
        private String loadDate;
        private String gender;
        private Integer flCurrent;
        private String dateChanged;
        private String policyHolderName;
        private String dpConsent;
        private String cdmpMember;
        private String fieldCareNavigation;
        private String medicalConsent;
        private String mumsClubMember;
        private String dob;
        private String policyType;
        private String onAntara;
        private String country;
        private String hashKey;
        private String countryName;
        private Integer isImported;
        private Long policyHolderId;
        private String mentalHealthProgram;

        
        public static ActisureCustomer fromResultSet(ResultSet rs) {
            try {
                ActisureCustomer customer = new ActisureCustomer();
                customer.customerSegmented = rs.getString("Customer_Segmented");
                customer.marketingConsent = rs.getString("MARKETING_CONSENT");
                
                Object dateLoadedObj = rs.getObject("_date_loaded");
                customer.dateLoaded = dateLoadedObj != null ? dateLoadedObj.toString() : null;

                customer.entityId = rs.getObject("ENTITYID") != null ? rs.getLong("ENTITYID") : null;
                customer.customerId = rs.getString("CUSTOMER_ID");
                customer.loadDate = rs.getString("Load_Date");
                customer.gender = rs.getString("Gender");
                customer.flCurrent = rs.getObject("FL_CURRENT") != null ? rs.getInt("FL_CURRENT") : null;

                Object dateChangedObj = rs.getObject("_date_changed");
                customer.dateChanged = dateChangedObj != null ? dateChangedObj.toString() : null;

                customer.policyHolderName = rs.getString("Policy_Holder_Name");
                customer.dpConsent = rs.getString("DP_CONSENT");
                customer.cdmpMember = rs.getString("CDMP_Member");
                customer.fieldCareNavigation = rs.getString("Field_Care_Navigation");
                customer.medicalConsent = rs.getString("MEDICAL_CONSENT");
                customer.mumsClubMember = rs.getString("Mums_Club_Member");
                customer.dob = rs.getString("Dob");
                customer.policyType = rs.getString("Policy_Type");
                customer.onAntara = rs.getString("On_Antara");
                customer.country = rs.getString("Country");
                customer.hashKey = rs.getString("HashKey");
                customer.countryName = rs.getString("Country_Name");
                customer.isImported = rs.getObject("IS_IMPORTED") != null ? rs.getInt("IS_IMPORTED") : null;
                customer.policyHolderId = rs.getObject("Policy_Holder_ID") != null ? rs.getLong("Policy_Holder_ID") : null;
                customer.mentalHealthProgram = rs.getString("Mental_Health_Program");

                return customer;
            } catch (Exception e) {
                return null;
            }
        }

        
        public void appendToJsonObject(MbElement itemObj) throws MbException {
            addIfNotNull(itemObj, "Customer_Segmented", customerSegmented);
            addIfNotNull(itemObj, "MARKETING_CONSENT", marketingConsent);
            addIfNotNull(itemObj, "_date_loaded", dateLoaded);
            addIfNotNull(itemObj, "ENTITYID", entityId);
            addIfNotNull(itemObj, "CUSTOMER_ID", customerId);
            addIfNotNull(itemObj, "Load_Date", loadDate);
            addIfNotNull(itemObj, "Gender", gender);
            addIfNotNull(itemObj, "FL_CURRENT", flCurrent);
            addIfNotNull(itemObj, "_date_changed", dateChanged);
            addIfNotNull(itemObj, "Policy_Holder_Name", policyHolderName);
            addIfNotNull(itemObj, "DP_CONSENT", dpConsent);
            addIfNotNull(itemObj, "CDMP_Member", cdmpMember);
            addIfNotNull(itemObj, "Field_Care_Navigation", fieldCareNavigation);
            addIfNotNull(itemObj, "MEDICAL_CONSENT", medicalConsent);
            addIfNotNull(itemObj, "Mums_Club_Member", mumsClubMember);
            addIfNotNull(itemObj, "Dob", dob);
            addIfNotNull(itemObj, "Policy_Type", policyType);
            addIfNotNull(itemObj, "On_Antara", onAntara);
            addIfNotNull(itemObj, "Country", country);
            addIfNotNull(itemObj, "HashKey", hashKey);
            addIfNotNull(itemObj, "Country_Name", countryName);
            addIfNotNull(itemObj, "IS_IMPORTED", isImported);
            addIfNotNull(itemObj, "Policy_Holder_ID", policyHolderId);
            addIfNotNull(itemObj, "Mental_Health_Program", mentalHealthProgram);
        }

        private void addIfNotNull(MbElement itemObj, String jsonKey, Object val) throws MbException {
            if (val != null) {
                if (val instanceof String) {
                    String strVal = ((String) val).trim();
                    if (!strVal.isEmpty()) {
                        itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, jsonKey, strVal);
                    }
                } else {
                    itemObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, jsonKey, val);
                }
            }
        }
    }

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
            // 1. Extract required query parameters
            String fromDate = getParam(inAssembly, "fromDate", "FromDate");
            String toDate = getParam(inAssembly, "toDate", "ToDate");
            String status = getParam(inAssembly, "status", "Status");

            boolean hasFromDate = (fromDate != null && !fromDate.trim().isEmpty());
            boolean hasToDate = (toDate != null && !toDate.trim().isEmpty());
            boolean hasStatus = (status != null && !status.trim().isEmpty());

            if (!hasFromDate || !hasToDate || !hasStatus) {
                sendAlternateResponse(alternateMessage, outLocalEnv, 400, false, 
                    "Required query parameters missing: fromDate, toDate, and status (NEW or UPDATED) must be provided.");
                alternate.propagate(alternateAssembly);
                return;
            }

            String cleanStatus = status.trim();
            if (!"NEW".equals(cleanStatus) && !"UPDATED".equals(cleanStatus)) {
                sendAlternateResponse(alternateMessage, outLocalEnv, 400, false, 
                    "Invalid status filter. Status must be case-sensitively 'NEW' or 'UPDATED'.");
                alternate.propagate(alternateAssembly);
                return;
            }

            // 2. Determine target date column using PostgreSQL double quotes
            String dateColumn = "NEW".equals(cleanStatus) ? "\"_date_loaded\"" : "\"_date_changed\"";

            // 3. Format boundary timestamps for the '< ?' range condition
            String startTimestamp = fromDate.trim().contains(" ") ? fromDate.trim() : fromDate.trim() + " 00:00:00";
            String endTimestamp;

            if (toDate.trim().contains(" ")) {
                endTimestamp = toDate.trim();
            } else {
                try {
                    LocalDate endDatePlusOne = LocalDate.parse(toDate.trim()).plusDays(1);
                    endTimestamp = endDatePlusOne.toString() + " 00:00:00";
                } catch (Exception parseEx) {
                    endTimestamp = toDate.trim() + " 23:59:59";
                }
            }

            // 4. Obtain JDBC Connection via ACE DB policy
            Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO);

            // 5. Run SQL Query
            String sql = "SELECT * FROM \"Actisure_Dim_Customer\" WHERE " + dateColumn + " >= ?::timestamp AND " + dateColumn + " < ?::timestamp";

            List<ActisureCustomer> customerList = new ArrayList<>();

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, startTimestamp);
                ps.setString(2, endTimestamp);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ActisureCustomer customer = ActisureCustomer.fromResultSet(rs);
                        if (customer != null) {
                            customerList.add(customer);
                        }
                    }
                }
            }

            // 6. Build the ACE Output Message Tree from POJO list
            MbElement root = outMessage.getRootElement();
            MbElement httpHeader = root.createElementAsLastChild("HTTPRequestHeader");
            httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

            MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
            jsonRoot.setSpecificType(MbJSON.ARRAY);

            for (ActisureCustomer customer : customerList) {
                MbElement itemObj = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, "Item", null);
                itemObj.setSpecificType(MbJSON.OBJECT);
                customer.appendToJsonObject(itemObj);
            }

            out.propagate(outAssembly);

        } catch (Exception e) {

            
            
        	System.err.println("Database Error: " + e.getMessage()); 
        	
        	sendAlternateResponse(alternateMessage, outLocalEnv, 500, false, "Internal Server Error: There was a problem establishing a connection to the given database URL.");
        	
        	 alternate.propagate(alternateAssembly);
        }
    }

    private void sendAlternateResponse(MbMessage altMessage, MbMessage altLocalEnv, int httpStatusCode, boolean success, String message) throws MbException {
        MbElement localEnvRoot = altLocalEnv.getRootElement();
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
            http.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "ReplyStatusCode", httpStatusCode);
        } else {
            replyCode.setValue(httpStatusCode);
        }

        MbElement root = altMessage.getRootElement();
        MbElement httpHeader = root.createElementAsLastChild("HTTPRequestHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");

        MbElement jsonRoot = root.createElementAsLastChild("JSON").createElementAsLastChild(MbElement.TYPE_NAME, "Data", null);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", String.valueOf(success));
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", message);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "statusCode", httpStatusCode);
        jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "timestamp", getCurrentTimestampIso());
    }

    private String getParam(MbMessageAssembly inAssembly, String... keys) {
        try {
            MbElement root = inAssembly.getLocalEnvironment().getRootElement();

            MbElement restParams = root.getFirstElementByPath("REST/Input/Parameters");
            if (restParams != null) {
                for (String key : keys) {
                    MbElement el = restParams.getFirstElementByPath(key);
                    if (el != null && el.getValueAsString() != null && !el.getValueAsString().trim().isEmpty()) {
                        return el.getValueAsString().trim();
                    }
                }
            }

            MbElement queryParams = root.getFirstElementByPath("HTTP/Input/QueryString");
            if (queryParams != null) {
                for (String key : keys) {
                    MbElement el = queryParams.getFirstElementByPath(key);
                    if (el != null && el.getValueAsString() != null && !el.getValueAsString().trim().isEmpty()) {
                        return el.getValueAsString().trim();
                    }
                }
            }
        } catch (Exception ignored) {}
        return null;
    }

    private String getCurrentTimestampIso() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TimeZone.getTimeZone("Africa/Nairobi"));
        return sdf.format(new Date());
    }
}