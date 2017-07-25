package com.uesleilima.spring.batch.integration.domain;

import java.math.BigDecimal;
import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * @author Ueslei Lima
 *
 */
@Entity
public class Entry {
	
	@Id
	private String source;
	private String destination;
	private BigDecimal amount;
	private Date date;
	
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public String getDestination() {
		return destination;
	}
	public void setDestination(String destination) {
		this.destination = destination;
	}
	public BigDecimal getAmount() {
		return amount;
	}
	public void setAmount(BigDecimal amount) {
		this.amount = amount;
	}
	public Date getDate() {
		return date;
	}
	public void setDate(Date date) {
		this.date = date;
	}
	
	@Override
	public String toString() {
		return String.format("Entry [ source=%s, destination=%s, amount=%f, date=%tc ]", source, destination, amount, date);
	}
}
