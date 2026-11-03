package com.jubilee.datalake.processor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.MbElement;
import com.ibm.broker.plugin.MbException;
import com.ibm.broker.plugin.MbJSON;
import com.ibm.broker.plugin.MbMessage;
import com.ibm.broker.plugin.MbMessageAssembly;
import com.ibm.broker.plugin.MbOutputTerminal;
import com.jubilee.datalake.exception.ValidationException;

public class GetCustomerMasterDataV3RequestProcessor extends MbJavaComputeNode {

    @Override
    public void evaluate(MbMessageAssembly inAssembly) throws MbException {
        MbOutputTerminal out = getOutputTerminal("out");
        MbOutputTerminal alt = getOutputTerminal("alternate");

        try {
            MbMessage outMessage = new MbMessage();
            MbMessageAssembly outAssembly = new MbMessageAssembly(inAssembly, outMessage);

            Map<String, String> queryParams = extractQueryParameters(inAssembly);
            String entityId = queryParams.get("EntityID");
            String country = queryParams.get("country");

            // Validate mandatory inputs
            if (entityId == null || entityId.trim().isEmpty() || country == null || country.trim().isEmpty()) {
                throw new ValidationException("Missing mandatory query parameters: 'EntityID' and 'country' are required.");
            }

            // Validate numeric EntityID
            long beneficiaryId;
            try {
                beneficiaryId = Long.parseLong(entityId.trim());
            } catch (NumberFormatException e) {
                throw new ValidationException("Invalid 'EntityID': must be a valid numeric value.");
            }

            // SQL query with explicit CASTs to prevent PostgreSQL type mismatches on parameter & JOINs
            String sql = "WITH CTE_LATEST_POLICY AS "
                       + "  (SELECT ROW_NUMBER() OVER (PARTITION BY \"PolicyId\" ORDER BY \"EffectiveDate\" DESC) ROW_NUM, "
                       + "          \"PolicyId\", \"EffectiveDate\", \"BeneficiaryId\" "
                       + "   FROM \"ACTISURE_POLICY_BENEFICIARIES\" "
                       + "   WHERE 1 = 1 "
                       + "     AND \"EffectiveDate\" <= CURRENT_TIMESTAMP "
                       + "     AND \"Country_Name\" = ? "
                       + "     AND \"BeneficiaryId\" = CAST(? AS BIGINT)), "
                       + "     CTE_MEMBER AS "
                       + "  (SELECT PB.\"PolicyId\", PB.\"EffectiveDate\", PB.\"BeneficiaryId\", PB.\"BeneficiaryName\", "
                       + "          PB.\"ID_Number\", PB.\"OriginalJoinDate\", PB.\"ParentId\", PB.\"Relationship\", "
                       + "          PB.\"Principal_Id\", PB.\"PrincipalName\", PB.\"PolicyHolderId\", PB.\"DOB\", "
                       + "          PB.\"PhoneNumber\", PB.\"MOBILE_NUMBER_OTP\", PB.\"Email1\", PB.\"Email2\", "
                       + "          PB.\"CardType\", PB.\"VIP\", PB.\"Elite\", PB.\"Country_Name\", SC.\"Scheme_Name\" "
                       + "   FROM CTE_LATEST_POLICY PL "
                       + "   JOIN \"ACTISURE_POLICY_BENEFICIARIES\" PB ON PL.\"PolicyId\" = PB.\"PolicyId\" "
                       + "   AND PL.\"EffectiveDate\" = PB.\"EffectiveDate\" "
                       + "   AND PL.\"BeneficiaryId\" = PB.\"BeneficiaryId\" "
                       + "   AND PL.ROW_NUM = 1 "
                       + "   LEFT JOIN \"Actisure_Dim_Scheme\" SC ON CAST(PB.\"PolicyHolderId\" AS VARCHAR) = CAST(SC.\"Scheme_ID\" AS VARCHAR) "
                       + "   AND PB.\"Country\" = SC.\"Country\" "
                       + "   WHERE PB.\"EffectiveDate\" <= CURRENT_TIMESTAMP "
                       + "     AND PB.\"Country_Name\" = ?) "
                       + "SELECT * FROM CTE_MEMBER";

            try (Connection conn = getJDBCType4Connection("{Database_Policies}:CrmStagingDbConnectionPoolManager", JDBC_TransactionType.MB_TRANSACTION_AUTO)) {
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {

                    stmt.setString(1, country);
                    stmt.setLong(2, beneficiaryId);
                    stmt.setString(3, country);

                    // Build Success JSON Tree
                    MbElement root = outMessage.getRootElement();
                    MbElement jsonRoot = root.createElementAsLastChild(MbJSON.PARSER_NAME);
                    MbElement dataObject = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, MbJSON.DATA_ELEMENT_NAME, null);

                    dataObject.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", true);

                    MbElement statusObj = dataObject.createElementAsLastChild(MbElement.TYPE_NAME, "status", null);
                    statusObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "code", 200);
                    statusObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", "OK");

                    MbElement detailsArray = dataObject.createElementAsLastChild(MbElement.TYPE_NAME, "details", null);
                    detailsArray.setSpecificType(MbJSON.ARRAY);

                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            MbElement item = detailsArray.createElementAsLastChild(MbElement.TYPE_NAME, MbJSON.ARRAY_ITEM_NAME, null);
                            item.setSpecificType(MbJSON.OBJECT);

                            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "beneficiaryId", rs.getString("BeneficiaryId"));
                            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "nationalId", rs.getString("ID_Number"));
                            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "principalName", rs.getString("PrincipalName"));
                            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "beneficiaryName", rs.getString("BeneficiaryName"));
                            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "taxNumber", null);
                            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "gender", "Male");

                            String email = rs.getString("Email1");
                            if (email == null) email = rs.getString("Email2");
                            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "email", email);

                            String phone = rs.getString("PhoneNumber");
                            if (phone == null) phone = rs.getString("MOBILE_NUMBER_OTP");
                            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "cellphone", phone);
                            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "mobileNumber", phone);

                            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "dob", rs.getString("DOB"));
                            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "country", rs.getString("Country_Name"));
                            item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "schemeName", rs.getString("Scheme_Name"));
                        }
                    }
                } catch (Exception queryEx) {
                    // Rollback on SQL failure to clear aborted transaction state on the pooled connection
                    try {
                        if (!conn.getAutoCommit()) {
                            conn.rollback();
                        }
                    } catch (Exception rollbackEx) {
                        // Ignore rollback exceptions
                    }
                    throw queryEx;
                }
            }

            out.propagate(outAssembly);

        } catch (ValidationException ve) {
            MbMessage errMessage = new MbMessage();
            MbMessageAssembly errAssembly = new MbMessageAssembly(inAssembly, errMessage);

            setHttpResponseCode(errAssembly, 400);
            buildErrorResponse(errMessage, ve.getMessage(), 400);
            alt.propagate(errAssembly);

        } catch (Exception e) {
            e.printStackTrace();

            MbMessage errMessage = new MbMessage();
            MbMessageAssembly errAssembly = new MbMessageAssembly(inAssembly, errMessage);

            setHttpResponseCode(errAssembly, 500);
            buildErrorResponse(errMessage, "Contact Jubilee integration team with the request payload and timestamp.", 500);
            alt.propagate(errAssembly);
        }
    }

    private void buildErrorResponse(MbMessage outMessage, String errorMessage, int code) throws MbException {
        MbElement root = outMessage.getRootElement();
        MbElement jsonRoot = root.createElementAsLastChild(MbJSON.PARSER_NAME);
        MbElement dataObject = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, MbJSON.DATA_ELEMENT_NAME, null);

        dataObject.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", false);

        MbElement statusObj = dataObject.createElementAsLastChild(MbElement.TYPE_NAME, "status", null);
        statusObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "code", code);
        statusObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", errorMessage);
    }

    private void setHttpResponseCode(MbMessageAssembly assembly, int statusCode) throws MbException {
        MbElement localEnv = assembly.getLocalEnvironment().getRootElement();
        MbElement destination = localEnv.getFirstElementByPath("Destination");
        if (destination == null) {
            destination = localEnv.createElementAsLastChild(MbElement.TYPE_NAME, "Destination", null);
        }
        MbElement http = destination.getFirstElementByPath("HTTP");
        if (http == null) {
            http = destination.createElementAsLastChild(MbElement.TYPE_NAME, "HTTP", null);
        }
        MbElement replyStatusCode = http.getFirstElementByPath("ReplyStatusCode");
        if (replyStatusCode == null) {
            http.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "ReplyStatusCode", statusCode);
        } else {
            replyStatusCode.setValue(statusCode);
        }
    }

    private Map<String, String> extractQueryParameters(MbMessageAssembly assembly) throws MbException {
        Map<String, String> params = new HashMap<>();
        MbElement localEnv = assembly.getLocalEnvironment().getRootElement();
        MbElement httpQuery = localEnv.getFirstElementByPath("HTTP/Input/QueryString");
        if (httpQuery != null) {
            MbElement child = httpQuery.getFirstChild();
            while (child != null) {
                params.put(child.getName(), child.getValueAsString());
                child = child.getNextSibling();
            }
        }
        return params;
    }
}