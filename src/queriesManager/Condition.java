package queriesManager;

import java.util.Vector;

import databaseManager.DynamicObject;
import queriesManager.QueryParser.ConditionType;

public abstract class Condition {
	public ConditionType conditionType;

	public static Condition makeCondition(String statement) {
		if (QueryParser.getConditionType(statement) == QueryParser.ConditionType.AND) {
			return new AndCondition(statement);
		} else if (QueryParser.getConditionType(statement) == QueryParser.ConditionType.OR) {
			return new OrCondition(statement);
		} else if (QueryParser.getConditionType(statement) == QueryParser.ConditionType.SIMPLE) {
			return new SimpleCondition(statement);
		}
		return null;
	}

	public boolean compare(Vector<DynamicObject> recordObjects, Vector<String> tableList) {
		return false;
	}
}
