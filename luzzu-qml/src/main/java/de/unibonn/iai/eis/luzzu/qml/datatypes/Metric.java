package de.unibonn.iai.eis.luzzu.qml.datatypes;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Metric {

	private final static String TYPEOF = "if ((quad.getObject().isURI()) && (quad.getObject().getURI().equals(%%VALUE%%))){ %%ACTION%% }";
	private final static String NORMAL = "if (%%VALUE%%){ %%ACTION%% }";
	
	private Set<String> imports = new HashSet<String>();
	private Set<String> variables = new HashSet<String>();
	private Rule rule;
	private Action action;
	
	public Metric(Rule rule, Action action){
		this.rule = rule;
		this.action = action;
	}
	
	public String toJavaDecleration(){
		if (action == Action.MAP){
			imports.add("import java.util.Map;");
			imports.add("import java.util.HashMap;");
			variables.add("private Map<Node, Set<String>> _hashMap = new HashMap<Node, Set<String>>();");
		}
		if (action == Action.COUNT){
			variables.add("private int counter = 0;");
		}
		
		StringBuilder sb = new StringBuilder();
		
		for(List<Condition> conditionLst : rule.getConditionList()){
			for(Condition condition : conditionLst){
				
				if (condition.getConditionType() == ConditionType.TYPEOF){
					String s = Metric.TYPEOF.replace("%%VALUE%%", condition.getRhs());
					if (action == Action.MAP){
						//the condition has another constraint
						s = s.replace("%%ACTION%%", "if (!(this._hashMap.containsKey(quad.getSubject()))) this._hashMap.put(quad.getSubject(), new HashSet<String>());");
					} else if (action == Action.COUNT){
						//the condition has no other constraint then possibly we have a count
						s = s.replace("%%ACTION%%","counter++;");
					}
					sb.append(s);
				}
				
				if (condition.getConditionType() == ConditionType.NORMAL){
					String s = Metric.NORMAL;
					String constraint = "";
					// check lhs
					if (this.isIRI(condition.getRhs())){
						if (condition.getLhs() == "?s") constraint += "quad.getSubject().getURI()";
						if (condition.getLhs() == "?p") constraint += "quad.getPredicate().getURI()";
						if (condition.getLhs() == "?o") constraint += "quad.getObject().getURI()";
					} else {
						if (condition.getLhs() == "?o") constraint += "quad.getObject().getLiteralValue()";
					}
					
					// check operator
					if (condition.getBooleanOperator().equals("==")) constraint += ".equals(%%CONSTRAINT%%)";
					if (condition.getBooleanOperator().equals("!=")) constraint = "!("+constraint+".equals(%%CONSTRAINT%%)"+")";
					
					// check rhs
					constraint = constraint.replace("%%CONSTRAINT%%", condition.getRhs());
					s = s.replace("%%VALUE%%", constraint);
					if (action == Action.MAP){
						//the condition has another constraint
						s = s.replace("%%ACTION%%", "if (!(this._hashMap.containsKey(quad.getSubject()))) this._hashMap.put(quad.getSubject(), new HashSet<String>());"+
						"Set<String> def = this._hashMap.get(quad.getSubject());"+
						"def.add(quad.getObject().getLiteralValue().toString());"+
						"this._hashMap.put(quad.getSubject(), def);");
					
					} else if (action == Action.COUNT){
						//the condition has no other constraint then possibly we have a count
						s =s.replace("%%ACTION%%","counter++;");
					}
					sb.append(s);
				}
			}
		}
		return sb.toString();
	}
	
	
	private boolean isIRI(String rhsCondition){
		return (rhsCondition.startsWith("<") && rhsCondition.endsWith(">"));	
	}
}
