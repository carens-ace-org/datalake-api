package com.jubilee.datalake.model;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

public class BenefitBalanceResponse {

    private Long benefitId;
    private String benefit;
    private BigDecimal balance;
    private String policyId;
    private String masterPolicyId;
    private String effectiveDate;

    // Default Constructor
    public BenefitBalanceResponse() {}

    // Full Parameter Constructor
    public BenefitBalanceResponse(Long benefitId, String benefit, BigDecimal balance, 
                                  String policyId, String masterPolicyId, String effectiveDate) {
        this.benefitId = benefitId;
        this.benefit = benefit;
        this.balance = balance;
        this.policyId = policyId;
        this.masterPolicyId = masterPolicyId;
        this.effectiveDate = effectiveDate;
    }

    // Getters and Setters
    public Long getBenefitId() {
        return benefitId;
    }

    public void setBenefitId(Long benefitId) {
        this.benefitId = benefitId;
    }

    public String getBenefit() {
        return benefit;
    }

    public void setBenefit(String benefit) {
        this.benefit = benefit;
    }

    public BigDecimal getBalance() {
        return balance;
    }

    public void setBalance(BigDecimal balance) {
        this.balance = balance;
    }

    public String getPolicyId() {
        return policyId;
    }

    public void setPolicyId(String policyId) {
        this.policyId = policyId;
    }

    public String getMasterPolicyId() {
        return masterPolicyId;
    }

    public void setMasterPolicyId(String masterPolicyId) {
        this.masterPolicyId = masterPolicyId;
    }

    public String getEffectiveDate() {
        return effectiveDate;
    }

    public void setEffectiveDate(String effectiveDate) {
        this.effectiveDate = effectiveDate;
    }

    /**
     * Converts the Model into a Map matching exact API casing specifications.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("BenefitId", this.benefitId != null ? this.benefitId : 0);
        map.put("Benefit", this.benefit != null ? this.benefit : "");
        map.put("Balance", this.balance != null ? this.balance : 0.00);
        return map;
    }
}