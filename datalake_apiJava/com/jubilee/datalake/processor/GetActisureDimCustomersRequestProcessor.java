package com.jubilee.datalake.processor;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.*;
import com.jubilee.datalake.model.ActisureCustomer;
import com.jubilee.datalake.model.QueryParams;
import com.jubilee.datalake.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class GetActisureDimCustomersRequestProcessor extends MbJavaComputeNode {

    private static final String DB_POLICY = "{Database_Policies}:CrmStagingDbConnectionPoolManager";

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
            QueryParams params = Utils.extractAndValidateParams(inAssembly);
            if (params.hasError()) {
                Utils.sendErrorResponse(alternateMessage, outLocalEnv, params.getErrorCode(), params.getErrorMessage());
                alternate.propagate(alternateAssembly);
                return;
            }

            List<ActisureCustomer> customerList = fetchCustomers(params);
            buildSuccessResponse(outMessage, customerList);
            out.propagate(outAssembly);

        } catch (Exception e) {
            System.err.println("Database Error: " + e.getMessage());
            Utils.sendErrorResponse(alternateMessage, outLocalEnv, 500, 
                "Internal Server Error: There was a problem establishing a connection to the given database URL.");
            alternate.propagate(alternateAssembly);
        }
    }

    

    private List<ActisureCustomer> fetchCustomers(QueryParams params) throws Exception {
        String dateColumn = "NEW".equals(params.status) ? "\"_date_loaded\"" : "\"_date_changed\"";
        String startTimestamp = formatStartTimestamp(params.fromDate);
        String endTimestamp = formatEndTimestamp(params.toDate);

        String sql = "SELECT * FROM \"Actisure_Dim_Customer\" WHERE " + dateColumn + " >= ?::timestamp AND " + dateColumn + " < ?::timestamp";
        List<ActisureCustomer> customerList = new ArrayList<>();

        try (Connection conn = getJDBCType4Connection(DB_POLICY, JDBC_TransactionType.MB_TRANSACTION_AUTO);
             PreparedStatement ps = conn.prepareStatement(sql)) {

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
        return customerList;
    }

    private void buildSuccessResponse(MbMessage outMessage, List<ActisureCustomer> customerList) throws MbException {
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
    }

    private String formatStartTimestamp(String inputDate) {
        return inputDate.contains(" ") ? inputDate : inputDate + " 00:00:00";
    }

    private String formatEndTimestamp(String inputDate) {
        if (inputDate.contains(" ")) {
            return inputDate;
        }
        try {
            LocalDate endDatePlusOne = LocalDate.parse(inputDate).plusDays(1);
            return endDatePlusOne.toString() + " 00:00:00";
        } catch (Exception parseEx) {
            return inputDate + " 23:59:59";
        }
    }



}