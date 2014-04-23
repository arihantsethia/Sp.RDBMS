package queriesManager;

import java.util.Vector;

import databaseManager.Attribute;
import databaseManager.Relation;

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

	public boolean executeOperation() {
		if (newCondition != null && !newCondition.equals("")) {
			intermediateOP = Operation.makeOperation("select " + projectionPart + " from " + tablePart + " where " + newCondition);
		}
		if (intermediateOP.executeOperation()) {
			return true;
		} else {
			return false;
		}
	}

}
