package queriesManager;

import java.util.Vector;

import databaseManager.Attribute;
import databaseManager.DynamicObject;
import databaseManager.Utility;
import queriesManager.QueryParser.DataType;
import queriesManager.QueryParser.OperatorType;

public class SimpleCondition extends Condition {
	private String leftOperand, rightOperand;
	private DataType leftData, rightData;
	private OperatorType operator;
	private int objectIndexLeft, objectIndexRight;
	private String leftNickName, rightNickName;
	private int attributeIndexLeft, attributeIndexRight;
	private String leftValue, rightValue;

	public SimpleCondition(String statement) {
		statement = statement.trim().substring(1, statement.trim().length() - 1).trim();
		operator = QueryParser.getOperatorType(statement);
		leftOperand = QueryParser.getLeftOperand(statement, operator);
		rightOperand = QueryParser.getRightOperand(statement, operator);
	}

	public boolean compare(Vector<DynamicObject> recordObjects, Vector<String> tableList) {
		getLeftOperandData(recordObjects, tableList);
		getRightOperandData(recordObjects, tableList);

		if (leftData == rightData) {
			if (leftData == QueryParser.DataType.Int) {

				int left = Integer.parseInt(leftValue);
				int right = Integer.parseInt(rightValue);

				if (operator == OperatorType.EQUAL && left == right) {
					return true;
				} else if (operator == OperatorType.GREATEQUAL && left >= right) {
					return true;
				} else if (operator == OperatorType.LESS && left < right) {
					return true;
				} else if (operator == OperatorType.LESSEQUAL && left <= right) {
					return true;
				} else if (operator == OperatorType.GREATER && left > right) {
					return true;
				}

			} else if (leftData == QueryParser.DataType.Char) {

				if (operator == OperatorType.EQUAL && leftValue.compareTo(rightValue) == 0) {
					return true;
				} else if (operator == OperatorType.GREATEQUAL && leftValue.compareTo(rightValue) >= 0) {
					return true;
				} else if (operator == OperatorType.LESS && leftValue.compareTo(rightValue) < 0) {
					return true;
				} else if (operator == OperatorType.LESSEQUAL && leftValue.compareTo(rightValue) <= 0) {
					return true;
				} else if (operator == OperatorType.GREATER && leftValue.compareTo(rightValue) > 0) {
					return true;
				}
			}
		}

		return false;
	}

	void getLeftOperandData(Vector<DynamicObject> recordObjects, Vector<String> tableList) {
		if (leftOperand.contains(".")) {
			leftNickName = Utility.getRelationName(leftOperand).trim();

			for (int i = 0; i < tableList.size(); i++) {
				if (Utility.getNickName(tableList.get(i)).equals(leftNickName)) {
					objectIndexLeft = i;
				}
			}

			for (int i = 0; i < recordObjects.get(objectIndexLeft).attributes.size(); i++) {
				if (recordObjects.get(objectIndexLeft).attributes.get(i).getName().equals(Utility.getNickName(leftOperand).trim())) {
					attributeIndexLeft = i;
				}
			}

			if (recordObjects.get(objectIndexLeft).attributes.get(attributeIndexLeft).getType() == Attribute.Type.Char) {
				leftData = QueryParser.DataType.Char;
				leftValue = (String) recordObjects.get(objectIndexLeft).obj[attributeIndexLeft];
			} else if (recordObjects.get(objectIndexLeft).attributes.get(attributeIndexLeft).getType() == Attribute.Type.Int) {
				leftData = QueryParser.DataType.Int;
				leftValue = ((Integer) recordObjects.get(objectIndexLeft).obj[attributeIndexLeft]).toString();
			}

		} else {
			if (Utility.isSameType("int", leftOperand)) {
				leftValue = leftOperand;
				leftData = QueryParser.DataType.Int;
			} else if (Utility.isSameType("char", leftOperand)) {
				leftValue = leftOperand.replace("\'", "");
				leftData = QueryParser.DataType.Char;
			}
		}
	}

	void getRightOperandData(Vector<DynamicObject> recordObjects, Vector<String> tableList) {
		if (rightOperand.contains(".")) {
			rightNickName = Utility.getRelationName(rightOperand).trim();

			for (int i = 0; i < tableList.size(); i++) {
				if (Utility.getNickName(tableList.get(i)).equals(rightNickName)) {
					objectIndexRight = i;
				}
			}

			for (int i = 0; i < recordObjects.get(objectIndexRight).attributes.size(); i++) {
				if (recordObjects.get(objectIndexRight).attributes.get(i).getName().equals(Utility.getNickName(rightOperand))) {
					attributeIndexRight = i;
				}
			}

			if (recordObjects.get(objectIndexRight).attributes.get(attributeIndexRight).getType() == Attribute.Type.Char) {
				rightData = QueryParser.DataType.Char;
				rightValue = (String) recordObjects.get(objectIndexRight).obj[attributeIndexRight];
			} else if (recordObjects.get(objectIndexRight).attributes.get(attributeIndexRight).getType() == Attribute.Type.Int) {
				rightData = QueryParser.DataType.Int;
				rightValue = ((Integer) recordObjects.get(objectIndexRight).obj[attributeIndexRight]).toString();
			}
		} else {
			if (Utility.isSameType("int", rightOperand)) {
				rightValue = rightOperand;
				rightData = QueryParser.DataType.Int;
			} else if (Utility.isSameType("char", rightOperand)) {
				rightValue = rightOperand.replace("\'", "");
				rightData = QueryParser.DataType.Char;
			}
		}
	}
}
