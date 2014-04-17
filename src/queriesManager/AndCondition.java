package queriesManager;

import java.util.Vector;

import databaseManager.DynamicObject;
import queriesManager.QueryParser.ConditionType;
import queriesManager.QueryParser.OperatorType;

public class AndCondition extends Condition {
	private Condition leftCondition ;
	private Condition rightCondition ;
	private String leftPart ;
	private String rightPart ;
	
	public AndCondition(String statement){
		conditionType  = ConditionType.AND ;
		leftPart = statement.trim().substring(1,statement.trim().indexOf("and")) ;
		rightPart = statement.trim().substring(statement.trim().indexOf("and")+3,statement.trim().length()-1) ;
		leftCondition = Condition.makeCondition(leftPart) ;
		rightCondition = Condition.makeCondition(rightPart);
	}
	
	public boolean compare(Vector<DynamicObject> recordObjects, Vector<String> tableList) {
		return leftCondition.compare(recordObjects, tableList) && rightCondition.compare(recordObjects, tableList) ;
	}
}
