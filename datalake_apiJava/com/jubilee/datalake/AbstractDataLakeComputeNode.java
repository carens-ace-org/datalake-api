package com.jubilee.datalake;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.ibm.broker.plugin.MbNode.JDBC_TransactionType;
import com.jubilee.datalake.exception.ValidationException;

import java.sql.*;
import java.util.*;

public abstract class AbstractDataLakeComputeNode extends MbJavaComputeNode {

    protected static final String DATASOURCE_NAME = "DATALAKE_DS";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void evaluate(MbMessageAssembly inAssembly) throws MbException {
        MbOutputTerminal out = getOutputTerminal("out");
        MbOutputTerminal alt = getOutputTerminal("alternate");

        MbMessage inMessage = inAssembly.getMessage();

        try {
            MbMessage outMessage = new MbMessage();
            MbMessageAssembly outAssembly = new MbMessageAssembly(inAssembly, outMessage);
           

            Connection conn = getJDBCType4Connection("{Database_Policies}:DATALAKE_CRM_DB", JDBC_TransactionType.MB_TRANSACTION_AUTO);
            conn.setReadOnly(true);

            Map<String, String> queryParams = extractQueryParameters(inAssembly);
            Map<String, Object> bodyParams = extractBodyParameters(inMessage);

            // Accepts List of POJOs or List of Maps
            List<?> resultList = processRequest(conn, queryParams, bodyParams);

            buildSuccessResponse(outMessage, resultList);
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

    // Return type changed to List<?> so subclasses can return List<POJO> or List<Map>
    protected abstract List<?> processRequest(
            Connection conn, Map<String, String> queryParams, Map<String, Object> bodyParams) throws Exception;
    

    protected void buildSuccessResponse(MbMessage outMessage, List<?> details) throws MbException {
        MbElement root = outMessage.getRootElement();
        MbElement jsonRoot = root.createElementAsLastChild(MbJSON.PARSER_NAME);
        MbElement dataObject = jsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, MbJSON.DATA_ELEMENT_NAME, null);

        dataObject.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "success", true);

        // "status": { "code": 200, "message": "OK" }
        MbElement statusObj = dataObject.createElementAsLastChild(MbElement.TYPE_NAME, "status", null);
        statusObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "code", 200);
        statusObj.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", "OK");

        // 1. Explicitly define "details" as a JSON ARRAY
        MbElement detailsArray = dataObject.createElementAsLastChild(MbElement.TYPE_NAME, "details", null);
        detailsArray.setSpecificType(MbJSON.ARRAY); 

        if (details != null) {
            for (Object obj : details) {
                // 2. Explicitly define each array item as a JSON OBJECT
                MbElement item = detailsArray.createElementAsLastChild(MbElement.TYPE_NAME, MbJSON.ARRAY_ITEM_NAME, null);
                item.setSpecificType(MbJSON.OBJECT); 

                @SuppressWarnings("unchecked")
                Map<String, Object> map = (obj instanceof Map) ? (Map<String, Object>) obj : MAPPER.convertValue(obj, Map.class);

                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    item.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, entry.getKey(), entry.getValue());
                }
            }
        }
    }

    protected void buildErrorResponse(MbMessage outMessage, String errorMessage, int code) throws MbException {
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

    protected Map<String, String> extractQueryParameters(MbMessageAssembly assembly) throws MbException {
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

    protected Map<String, Object> extractBodyParameters(MbMessage inMessage) throws MbException {
        Map<String, Object> body = new HashMap<>();
        MbElement jsonBody = inMessage.getRootElement().getFirstElementByPath("JSON/Data");
        if (jsonBody != null) {
            MbElement child = jsonBody.getFirstChild();
            while (child != null) {
                body.put(child.getName(), child.getValueAsString());
                child = child.getNextSibling();
            }
        }
        return body;
    }


}