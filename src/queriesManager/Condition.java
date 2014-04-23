package queriesManager;

import java.util.Vector;

import databaseManager.DynamicObject;
import queriesManager.QueryParser.ConditionType;

/**
 * 
 * The instance of "Condition" class is called by Operation Class whenever we
 * want to evaluate condition query which may have either a SimpleCondition or
 * ComplexCondition. It is a Intermediate Class which calls other condition
 * classes to evaluates the expression.
 * 
 */
public abstract class Condition {
	public ConditionType conditionType;

	/**
	 * Constructor of Condition Class it takes where expression as argument and
	 * Check the Condition Type and return Condition Instance according to that.
	 * 
	 * @param statement
	 *            takes where condition as argument.
	 */

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

	/**
	 * This function is overloaded by Other Derived Classes. it takes as
	 * argument list of record and table tuples that will be use to evaluate
	 * that condition.
	 */
	public boolean compare(Vector<DynamicObject> recordObjects, Vector<String> tableList) {
		return false;
	}
}
