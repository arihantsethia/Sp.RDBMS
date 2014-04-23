package queriesManager;

import java.util.Vector;

import queriesManager.QueryParser.ConditionType;
import databaseManager.DynamicObject;

/**
 * 
 * The instance of "OrCondition" class is called by Condition Class whenever we
 * want to evaluate condition query which have 'or' logical operator . It is a
 * Intermediate Class which calls other condition classes to evaluates the
 * expression.
 * 
 */
public class OrCondition extends Condition {
	private Condition leftCondition;
	private Condition rightCondition;
	private String leftPart;
	private String rightPart;

	/**
	 * Constructor of OrCondition Class it takes where expression as argument
	 * and divide it in leftCondition and rightCondition accordingly.
	 * 
	 * @param statement
	 *            takes where condition as argument.
	 */
	public OrCondition(String statement) {
		conditionType = ConditionType.OR;
		leftPart = statement.trim().substring(1, statement.trim().indexOf("or"));
		rightPart = statement.trim().substring(statement.trim().indexOf("or") + 3, statement.length() - 1);
		leftCondition = Condition.makeCondition(leftPart);
		rightCondition = Condition.makeCondition(rightPart);
	}

	/**
	 * it takes as argument list of record and table tuples that will be use to
	 * evaluate that condition.
	 */
	public boolean compare(Vector<DynamicObject> recordObjects, Vector<String> tableList) {
		return leftCondition.compare(recordObjects, tableList) || rightCondition.compare(recordObjects, tableList);
	}
}
