package com.jubilee.datalake.model;

public class QueryParams {
	public final String fromDate;
	public final String toDate;
    public final String status;
    public final boolean error;
    public final int errorCode;
    public final String errorMessage;

    public QueryParams(String fromDate, String toDate, String status, boolean error, int errorCode, String errorMessage) {
        this.fromDate = fromDate;
        this.toDate = toDate;
        this.status = status;
        this.error = error;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public static QueryParams success(String fromDate, String toDate, String status) {
        return new QueryParams(fromDate, toDate, status, false, 0, null);
    }

    public static QueryParams error(int errorCode, String errorMessage) {
        return new QueryParams(null, null, null, true, errorCode, errorMessage);
    }

    public boolean hasError() {
        return error;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}