package com.jubilee.datalake.utils;

import com.ibm.broker.plugin.*;
import com.jubilee.datalake.model.QueryParams;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Utils {

    private static final String TIMEZONE_NAIROBI = "Africa/Nairobi";

    public static String getParam(MbMessageAssembly assembly, String... keys) {
        try {
            MbElement root = assembly.getLocalEnvironment().getRootElement();
            String val = searchParamInPath(root, "REST/Input/Parameters", keys);
            if (val != null) {
                return val;
            }
            return searchParamInPath(root, "HTTP/Input/QueryString", keys);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String searchParamInPath(MbElement root, String path, String[] keys) {
        try {
            MbElement parent = root.getFirstElementByPath(path);
            if (parent == null) {
                return null;
            }
            for (String key : keys) {
                String val = extractElementValue(parent, key);
                if (val != null) {
                    return val;
                }
            }
        } catch (MbException ignored) {}
        return null;
    }

    private static String extractElementValue(MbElement parent, String key) {
        try {
            MbElement el = parent.getFirstElementByPath(key);
            if (el == null) {
                return null;
            }
            String val = el.getValueAsString();
            return isEmpty(val) ? null : val.trim();
        } catch (MbException e) {
            return null;
        }
    }

    public static void sendErrorResponse(MbMessage altMsg, MbMessage altEnv, int code, String msg) throws MbException {
        MbElement localEnvRoot = altEnv.getRootElement();
        MbElement httpDest = getOrCreateHttpDestination(localEnvRoot);
        updateReplyStatusCode(httpDest, code);

        MbElement root = altMsg.getRootElement();
        addHttpReplyHeader(root);
        addErrorJsonPayload(root, code, msg);
    }

    private static MbElement getOrCreateHttpDestination(MbElement localEnvRoot) throws MbException {
        MbElement dest = getOrCreateChild(localEnvRoot, "Destination");
        return getOrCreateChild(dest, "HTTP");
    }

    private static MbElement getOrCreateChild(MbElement parent, String name) throws MbException {
        MbElement child = parent.getFirstElementByPath(name);
        if (child == null) {
            child = parent.createElementAsLastChild(MbElement.TYPE_NAME, name, null);
        }
        return child;
    }

    private static void updateReplyStatusCode(MbElement httpDest, int code) throws MbException {
        MbElement replyCode = httpDest.getFirstElementByPath("ReplyStatusCode");
        if (replyCode == null) {
            httpDest.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "ReplyStatusCode", code);
        } else {
            replyCode.setValue(code);
        }
    }

    private static void addHttpReplyHeader(MbElement root) throws MbException {
        MbElement httpHeader = root.createElementAsLastChild("HTTPReplyHeader");
        httpHeader.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json");
    }

    private static void addErrorJsonPayload(MbElement root, int code, String msg) throws MbException {
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
        if (isEmpty(val)) {
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