package queriesManager;

import java.util.Vector;

import databaseManager.Attribute;
import databaseManager.Relation;

/**
 * 
 * The instance of "CondOperation" class is called whenever we want to execute
 * Conditional Join query. it calls instance of Select Class to evaluate final expression.
 * 
 */
public class CondJoin extends Operation {
	protected int tableCount;
	protected Vector<String> tableList;
	protected Relation relation;
	protected long relationId;
	protected String nickName;
	protected Operation intermediateOP;
	protected Vector<String> commonAttribute;
	protected Vector<String> restAttribute;
	protected Vector<Integer> commonAttributeSize;
	protected Vector<Attribute.Type> commonAttributeType;
	protected String newCondition, projectionPart, tablePart;

	/**
	 * This constructor will be called when we want to create object of class
	 * CondOperation It takes input query as arguments and split it into
	 * projectionPart , tablePart and newCondition. 
	 */
	public CondJoin(String statement) {
		setType(QueryParser.OperationType.CONDJOIN);
		Vector<String> stmtParts = QueryParser.statementParts(statement, "select");
		projectionPart = stmtParts.elementAt(0);
		tablePart = stmtParts.elementAt(1).substring(0, stmtParts.elementAt(1).indexOf("on")).replace("join", ",").trim();
		tablePart = tablePart.replace("(", "").replace(")", "").trim();
		tableList = QueryParser.getSelectTableList(tablePart);
		if (stmtParts.size() == 3) {
			newCondition = "(" + stmtParts.elementAt(1).substring(stmtParts.elementAt(1).indexOf("on") + 2) + " and " + stmtParts.elementAt(2) + " )";
		} else {
			newCondition = stmtParts.elementAt(1).substring(stmtParts.elementAt(1).indexOf("on") + 2);
		}
	}

	/**
	 * It create intermediate select query to evaluate operation and 
	 * Execute operation of that new Intermediate Class.
	 */
	public boolean executeOperation() {
		String fQuery = "";
		if (newCondition == null || newCondition.equals("")) {
			fQuery = "select " + projectionPart + " from " + tablePart + " where " + newCondition;
		}
		if(QueryParser.isSelectStatementQuery(fQuery)){
			intermediateOP = Operation.makeOperation(fQuery);
			if (intermediateOP.executeOperation()) {
				return true;
			} else {
				return false;
			}
		}else{
			return false;
		}
	}

}
