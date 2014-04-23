package queriesManager;

import java.util.Vector;

import queriesManager.QueryParser.ConditionType;
import databaseManager.DynamicObject;

public class OrCondition extends Condition {
	private Condition leftCondition;
	private Condition rightCondition;
	private String leftPart;
	private String rightPart;

	public OrCondition(String statement) {
		conditionType = ConditionType.OR;
		leftPart = statement.trim().substring(1, statement.trim().indexOf("or"));
		rightPart = statement.trim().substring(statement.trim().indexOf("or") + 3, statement.length() - 1);
		leftCondition = Condition.makeCondition(leftPart);
		rightCondition = Condition.makeCondition(rightPart);
	}

	public boolean compare(Vector<DynamicObject> recordObjects, Vector<String> tableList) {
		return leftCondition.compare(recordObjects, tableList) || rightCondition.compare(recordObjects, tableList);
	}
}
