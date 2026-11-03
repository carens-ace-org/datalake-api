package com.jubilee.datalake.model;

import java.sql.ResultSet;

import com.ibm.broker.plugin.MbElement;
import com.ibm.broker.plugin.MbException;

public class ActisureCustomer {
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
                ActisureCustomer c = new ActisureCustomer();
                c.customerSegmented = rs.getString("Customer_Segmented");
                c.marketingConsent = rs.getString("MARKETING_CONSENT");
                c.dateLoaded = getString(rs, "_date_loaded");
                c.entityId = getLong(rs, "ENTITYID");
                c.customerId = rs.getString("CUSTOMER_ID");
                c.loadDate = rs.getString("Load_Date");
                c.gender = rs.getString("Gender");
                c.flCurrent = getInt(rs, "FL_CURRENT");
                c.dateChanged = getString(rs, "_date_changed");
                c.policyHolderName = rs.getString("Policy_Holder_Name");
                c.dpConsent = rs.getString("DP_CONSENT");
                c.cdmpMember = rs.getString("CDMP_Member");
                c.fieldCareNavigation = rs.getString("Field_Care_Navigation");
                c.medicalConsent = rs.getString("MEDICAL_CONSENT");
                c.mumsClubMember = rs.getString("Mums_Club_Member");
                c.dob = rs.getString("Dob");
                c.policyType = rs.getString("Policy_Type");
                c.onAntara = rs.getString("On_Antara");
                c.country = rs.getString("Country");
                c.hashKey = rs.getString("HashKey");
                c.countryName = rs.getString("Country_Name");
                c.isImported = getInt(rs, "IS_IMPORTED");
                c.policyHolderId = getLong(rs, "Policy_Holder_ID");
                c.mentalHealthProgram = rs.getString("Mental_Health_Program");
                return c;
            } catch (Exception e) {
                return null;
            }
        }

        private static String getString(ResultSet rs, String col) throws Exception {
            Object obj = rs.getObject(col);
            return obj != null ? obj.toString() : null;
        }

        private static Long getLong(ResultSet rs, String col) throws Exception {
            Object obj = rs.getObject(col);
            return obj != null ? rs.getLong(col) : null;
        }

        private static Integer getInt(ResultSet rs, String col) throws Exception {
            Object obj = rs.getObject(col);
            return obj != null ? rs.getInt(col) : null;
        }

        public void appendToJsonObject(MbElement itemObj) throws MbException {
            add(itemObj, "Customer_Segmented", customerSegmented);
            add(itemObj, "MARKETING_CONSENT", marketingConsent);
            add(itemObj, "_date_loaded", dateLoaded);
            add(itemObj, "ENTITYID", entityId);
            add(itemObj, "CUSTOMER_ID", customerId);
            add(itemObj, "Load_Date", loadDate);
            add(itemObj, "Gender", gender);
            add(itemObj, "FL_CURRENT", flCurrent);
            add(itemObj, "_date_changed", dateChanged);
            add(itemObj, "Policy_Holder_Name", policyHolderName);
            add(itemObj, "DP_CONSENT", dpConsent);
            add(itemObj, "CDMP_Member", cdmpMember);
            add(itemObj, "Field_Care_Navigation", fieldCareNavigation);
            add(itemObj, "MEDICAL_CONSENT", medicalConsent);
            add(itemObj, "Mums_Club_Member", mumsClubMember);
            add(itemObj, "Dob", dob);
            add(itemObj, "Policy_Type", policyType);
            add(itemObj, "On_Antara", onAntara);
            add(itemObj, "Country", country);
            add(itemObj, "HashKey", hashKey);
            add(itemObj, "Country_Name", countryName);
            add(itemObj, "IS_IMPORTED", isImported);
            add(itemObj, "Policy_Holder_ID", policyHolderId);
            add(itemObj, "Mental_Health_Program", mentalHealthProgram);
        }

        private void add(MbElement parent, String key, Object val) throws MbException {
            if (val == null) return;
            if (val instanceof String) {
                String trimmed = ((String) val).trim();
                if (!trimmed.isEmpty()) {
                    parent.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, key, trimmed);
                }
            } else {
                parent.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, key, val);
            }
        }
    }