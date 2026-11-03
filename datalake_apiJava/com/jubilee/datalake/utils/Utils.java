package com.jubilee.datalake.utils;

import com.ibm.broker.plugin.*;
import com.jubilee.datalake.model.QueryParams;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Utils {

    private static final String DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.S";
    private static final String TIMEZONE_NAIROBI = "Africa/Nairobi";

    
    public static String getParam(MbMessageAssembly assembly, String... keys) {
        try {
            MbElement root = assembly.getLocalEnvironment().getRootElement();

            
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

    
    public static void sendErrorResponse(MbMessage altMsg, MbMessage altEnv, int code, String msg) throws MbException {
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
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "message", msg);
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "statusCode", code);
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "responseCode", "ERROR");
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "status", "99");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TimeZone.getTimeZone(TIMEZONE_NAIROBI));
        json.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "timestamp", sdf.format(new Date()));
    }
    
    public static QueryParams extractAndValidateParams(MbMessageAssembly inAssembly) {
        String fromDate = Utils.getParam(inAssembly, "fromDate", "FromDate");
        String toDate = Utils.getParam(inAssembly, "toDate", "ToDate");
        String status = Utils.getParam(inAssembly, "status", "Status");

        if (isEmpty(fromDate) || isEmpty(toDate) || isEmpty(status)) {
            return QueryParams.error(400, "Required query parameters missing: fromDate, toDate, and status (NEW or UPDATED) must be provided.");
        }

        String cleanStatus = status.trim();
        if (!"NEW".equals(cleanStatus) && !"UPDATED".equals(cleanStatus)) {
            return QueryParams.error(400, "Invalid status filter. Status must be case-sensitively 'NEW' or 'UPDATED'.");
        }

        return QueryParams.success(fromDate.trim(), toDate.trim(), cleanStatus);
    }
    

    private static boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }
    
    public static int parsePaginationParam(MbMessageAssembly inAssembly, String key, int defaultVal, int minVal) {
        String val = getParam(inAssembly, key);
        if (val == null || val.trim().isEmpty()) {
            return defaultVal;
        }
        try {
            int parsed = Integer.parseInt(val.trim());
            return parsed >= minVal ? parsed : defaultVal;
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }
    
}