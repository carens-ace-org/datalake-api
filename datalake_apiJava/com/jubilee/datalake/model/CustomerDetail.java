package com.jubilee.datalake.model;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CustomerDetail {

    private String beneficiaryId;
    private String principalName;
    private String beneficiaryName;
    private String nationalIdNumber;
    private String taxIdNumber;
    private String country;
   

    // Factory method to map DB ResultSet directly to the POJO
    public static CustomerDetail fromResultSet(ResultSet rs) throws SQLException {
        CustomerDetail detail = new CustomerDetail();
        detail.setBeneficiaryId(String.valueOf(rs.getObject("CUSTOMER_ID")));
        detail.setPrincipalName(rs.getString("CUSTOMER_NAME"));
        detail.setBeneficiaryName(rs.getString("CUSTOMER_NAME"));
        detail.setNationalIdNumber(rs.getString("NATIONAL_ID"));
        detail.setTaxIdNumber(rs.getString("TAX_NUMBER"));
        detail.setCountry(rs.getString("COUNTRY"));
        return detail;
    }

    // Getters and Setters
    public String getBeneficiaryId() { return beneficiaryId; }
    public void setBeneficiaryId(String beneficiaryId) { this.beneficiaryId = beneficiaryId; }

    public String getPrincipalName() { return principalName; }
    public void setPrincipalName(String principalName) { this.principalName = principalName; }

    public String getBeneficiaryName() { return beneficiaryName; }
    public void setBeneficiaryName(String beneficiaryName) { this.beneficiaryName = beneficiaryName; }

    public String getNationalIdNumber() { return nationalIdNumber; }
    public void setNationalIdNumber(String nationalIdNumber) { this.nationalIdNumber = nationalIdNumber; }

    public String getTaxIdNumber() { return taxIdNumber; }
    public void setTaxIdNumber(String taxIdNumber) { this.taxIdNumber = taxIdNumber; }

    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }
}