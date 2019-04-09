package com.project.etl.beans;

public class Query {
	
	private String select;
	private String filter;
	private Query previous1 = null;
	private Query previous2 = null;
	private String previousFile1 = null;
	private String previousFile2 = null;
	
	public String getSelect() {
		return select;
	}
	public void setSelect(String select) {
		this.select = select;
	}
	public String getFilter() {
		return filter;
	}
	public void setFilter(String filter) {
		this.filter = filter;
	}
	public Query getPrevious1() {
		return previous1;
	}
	public void setPrevious1(Query previous1) {
		this.previous1 = previous1;
	}
	public Query getPrevious2() {
		return previous2;
	}
	public void setPrevious2(Query previous2) {
		this.previous2 = previous2;
	}
	public String getPreviousFile1() {
		return previousFile1;
	}
	public void setPreviousFile1(String previousFile1) {
		this.previousFile1 = previousFile1;
	}
	public String getPreviousFile2() {
		return previousFile2;
	}
	public void setPreviousFile2(String previousFile2) {
		this.previousFile2 = previousFile2;
	}
	
	
}
