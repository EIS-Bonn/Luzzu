package de.unibonn.iai.eis.luzzu.qml.datatypes;

import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class Metric {

	private final static String TYPEOF = "if ((quad.getObject().isURI()) && (quad.getObject().getURI().equals(%%VALUE%%))){ %%ACTION%% }";
	private final static String NORMAL = "if (%%VALUE%%){ %%ACTION%% }";
	private final static String IMPORT = "import %%package%%;";
	
	private Set<String> imports = new HashSet<String>();
	private Set<String> variables = new HashSet<String>();
	private Rule rule;
	private Action action;
	
	public Metric(Rule rule, Action action){
		this.rule = rule;
		this.action = action;
	}
	
	public String getImports(){
		StringBuilder sb = new StringBuilder();
		for(String s : imports){
			sb.append(s);
			sb.append(System.getProperty("line.separator"));
		}
		return sb.toString();
	}
	
	public String getVariables(){
		StringBuilder sb = new StringBuilder();
		for(String s : variables){
			sb.append(s);
			sb.append(System.getProperty("line.separator"));
		}
		return sb.toString();
	}
	
	public String getComputeFunction(){
		if (action == Action.MAP){
			imports.add("import java.util.Map;");
			imports.add("import java.util.HashMap;");
			imports.add("import com.hp.hpl.jena.graph.Node;");
			imports.add("import com.hp.hpl.jena.rdf.model.Resource;");
			imports.add("import com.hp.hpl.jena.sparql.core.Quad;");
			imports.add("import java.util.Set;");
			imports.add("import java.util.HashSet;");

			variables.add("private Map<Node, Set<String>> _hashMap = new HashMap<Node, Set<String>>();");
		}
		if (action == Action.COUNT){
			variables.add("private int counter = 0;");
		}
		
		StringBuilder sb = new StringBuilder();
		
		for(List<Condition> conditionLst : rule.getConditionList()){
			for(Condition condition : conditionLst){
				
				if (condition.getConditionType() == ConditionType.TYPEOF){
					String s = Metric.TYPEOF.replace("%%VALUE%%", condition.getRhs().replace("<","\"").replace(">", "\""));
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
					constraint = constraint.replace("%%CONSTRAINT%%", condition.getRhs().replace("<","\"").replace(">", "\""));
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
	
	public String actionToJava(){
		StringBuilder sb = new StringBuilder();
		if (action == Action.MAP){
			sb.append("int entitiesWithoutTerms = 0;");
			sb.append("for (Node n : _hashMap.keySet()){");
			sb.append("entitiesWithoutTerms += (this._hashMap.get(n).size() > 0) ? 1 : 0;");
			sb.append("}");
			sb.append("return  (double) entitiesWithoutTerms / (double) _hashMap.keySet().size();");
			
		}

		if (action == Action.COUNT){
			sb.append("return counter;");
		}
		
		return sb.toString();
	}
	
	private boolean isIRI(String rhsCondition){
		return (rhsCondition.startsWith("<") && rhsCondition.endsWith(">"));	
	}
}
