package de.unibonn.iai.eis.luzzu.qml.datatypes;

public class Condition {
	private ConditionType conditionType;
	private String lhs;
	private String booleanOperator;
	private String rhs;
	private String logicalOperator;
	
	public ConditionType getConditionType() {
		return conditionType;
	}
	
	public void setConditionType(ConditionType conditionType) {
		this.conditionType = conditionType;
	}

	public String getLhs() {
		return lhs;
	}

	public void setLhs(String lhs) {
		this.lhs = lhs;
	}

	public String getBooleanOperator() {
		return booleanOperator;
	}

	public void setBooleanOperator(String booleanOperator) {
		this.booleanOperator = booleanOperator;
	}

	public String getRhs() {
		return rhs;
	}

	public void setRhs(String rhs) {
		this.rhs = rhs;
	}

	public String getLogicalOperator() {
		return logicalOperator;
	}

	public void setLogicalOperator(String logicalOperator) {
		this.logicalOperator = logicalOperator;
	}
}

