package de.unibonn.iai.eis.luzzu.qml.datatypes;

import java.util.ArrayList;
import java.util.List;

public class Rule {

	private String declerativeRule = "";
	private List<ArrayList<Condition>> conditionList = new ArrayList<ArrayList<Condition>>();
	
	public String getDeclerativeRule() {
		return declerativeRule;
	}
	public void setDeclerativeRule(String declerativeRule) {
		this.declerativeRule = declerativeRule;
	}
	public List<ArrayList<Condition>> getConditionList() {
		return conditionList;
	}
	public void setConditionList(List<ArrayList<Condition>> conditionList) {
		this.conditionList = conditionList;
	}
	
	
}
