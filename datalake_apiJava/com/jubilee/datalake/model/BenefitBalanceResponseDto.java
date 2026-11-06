package com.jubilee.datalake.model;

import java.math.BigDecimal;

public class BenefitBalanceResponseDto {

    private Long benefitId;
    private String benefit;
    private BigDecimal balance;
    private String currency;

    public BenefitBalanceResponseDto() {}

    public BenefitBalanceResponseDto(Long benefitId, String benefit, BigDecimal balance, String currency) {
        this.benefitId = benefitId;
        this.benefit = benefit;
        this.balance = balance;
        this.currency = currency;
    }

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

	public String getCurrency() {
		return currency;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}
}