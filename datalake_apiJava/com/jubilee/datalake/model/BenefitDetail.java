package com.jubilee.datalake.model;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BenefitDetail {
    private long subBenefitId;
    private String subBenefitName;
    private BigDecimal limit;
    private BigDecimal utilised;
    private BigDecimal available;
    private String currency;

    public BenefitDetail() {}

    public BenefitDetail(long subBenefitId, String subBenefitName, BigDecimal limit, 
                         BigDecimal utilised, BigDecimal available, String currency) {
        this.subBenefitId = subBenefitId;
        this.subBenefitName = subBenefitName;
        this.limit = limit;
        this.utilised = utilised;
        this.available = available;
        this.currency = currency;
    }

    /**
     * Factory method mapping PostgreSQL ResultSet directly into the BenefitDetail object.
     */
    public static BenefitDetail fromResultSet(ResultSet rs) throws SQLException {
        long subBenefitId = rs.getLong("SubBenefitId");
        String subBenefitName = rs.getString("SubBenefit");
        
        BigDecimal limit = rs.getBigDecimal("BenefitDetailAllocatedAmount");
        BigDecimal utilised = rs.getBigDecimal("ClaimAmount");
        BigDecimal available = rs.getBigDecimal("BALANCE");
        
        String country = rs.getString("Country");
        String currency = (country == null || country.trim().isEmpty()) ? "KES" : country.trim();

        return new BenefitDetail(
            subBenefitId,
            subBenefitName != null ? subBenefitName.trim() : "",
            limit != null ? limit : BigDecimal.ZERO,
            utilised != null ? utilised : BigDecimal.ZERO,
            available != null ? available : BigDecimal.ZERO,
            currency
        );
    }

    // Getters and Setters
    public long getSubBenefitId() { return subBenefitId; }
    public void setSubBenefitId(long subBenefitId) { this.subBenefitId = subBenefitId; }

    public String getSubBenefitName() { return subBenefitName; }
    public void setSubBenefitName(String subBenefitName) { this.subBenefitName = subBenefitName; }

    public BigDecimal getLimit() { return limit; }
    public void setLimit(BigDecimal limit) { this.limit = limit; }

    public BigDecimal getUtilised() { return utilised; }
    public void setUtilised(BigDecimal utilised) { this.utilised = utilised; }

    public BigDecimal getAvailable() { return available; }
    public void setAvailable(BigDecimal available) { this.available = available; }

    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }
}