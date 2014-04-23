package queriesManager;

import java.util.Vector;
import databaseManager.DynamicObject;
import queriesManager.QueryParser.ConditionType;

/**
 * 
 * The instance of "AndCondition" class is called by Condition Class whenever we
 * want to evaluate condition query which have 'and' logical operator . It is a
 * Intermediate Class which calls other condition classes to evaluates the
 * expression.
 * 
 */
public class AndCondition extends Condition {
	private Condition leftCondition;
	private Condition rightCondition;
	private String leftPart;
	private String rightPart;

	/**
	 * Constructor of AndCondition Class it takes where expression as argument
	 * and divide it in leftCondition and rightCondition accordingly.
	 * 
	 * @param statement
	 *            takes where condition as argument.
	 */
	public AndCondition(String statement) {
		conditionType = ConditionType.AND;
		leftPart = statement.trim().substring(1, statement.trim().indexOf("and"));
		rightPart = statement.trim().substring(statement.trim().indexOf("and") + 3, statement.trim().length() - 1);
		leftCondition = Condition.makeCondition(leftPart);
		rightCondition = Condition.makeCondition(rightPart);
	}

	/**
	 * it takes as argument list of record and table tuples that will be use to
	 * evaluate that condition.
	 */
	@Override
	public boolean compare(Vector<DynamicObject> recordObjects, Vector<String> tableList) {
		return leftCondition.compare(recordObjects, tableList) && rightCondition.compare(recordObjects, tableList);
	}
}
