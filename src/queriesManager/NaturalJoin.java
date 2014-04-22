package queriesManager;

import java.util.Vector;

import databaseManager.Attribute;
import databaseManager.ObjectHolder;
import databaseManager.Relation;
import databaseManager.Utility;

public class NaturalJoin extends Operation {
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

	public NaturalJoin(String statement) {
		setType(QueryParser.OperationType.NATURALJOIN);
		Vector<String> stmtParts = QueryParser.statementParts(statement, "select");
		projectionPart = stmtParts.elementAt(0);
		tablePart = stmtParts.elementAt(1).replace("join", ",").trim();
		tablePart = tablePart.replace("natural", "");
		tableList = QueryParser.getJoinTableList(stmtParts.elementAt(1), "natural");
		tableCount = tableList.size();

		this.getCommonAttribute();
		this.getRestAttribute();
		this.getProjectionParts(projectionPart);

		if (stmtParts.size() == 3) {
			this.addCondition(stmtParts.elementAt(2));
		} else {
			this.addCondition(null);
		}

	}

	public boolean executeOperation() {

		if (newCondition == null || newCondition.equals("")) {
			intermediateOP = Operation.makeOperation("select " + projectionPart + " from " + tablePart);
		} else {
			intermediateOP = Operation.makeOperation("select " + projectionPart + " from " + tablePart + " where " + newCondition);
		}
		if (intermediateOP.executeOperation()) {
			return true;
		} else {
			return false;
		}
	}

	void getCommonAttribute() {

		commonAttribute = new Vector<String>();
		commonAttributeType = new Vector<Attribute.Type>();
		commonAttributeSize = new Vector<Integer>();

		for (int i = 0; i < tableList.size(); i++) {

			nickName = Utility.getNickName(tableList.get(i));
			relationId = ObjectHolder.getObjectHolder().getRelationId(Utility.getRelationName(tableList.elementAt(i)));
			relation = (Relation) ObjectHolder.getObjectHolder().getObject(relationId);

			if (i == 0) {
				for (int j = 0; j < relation.getAttributesCount(); j++) {
					commonAttribute.addElement(nickName + "." + relation.getAttributes().get(j).getName());
					commonAttributeType.addElement(relation.getAttributes().get(j).getType());
					commonAttributeSize.addElement(relation.getAttributes().get(j).getAttributeSize());
				}
			} else {

				for (int j = 0; j < commonAttribute.size(); j++) {
					boolean isExist = false;
					for (int k = 0; k < relation.getAttributes().size(); k++) {
						if (Utility.getNickName(commonAttribute.get(j)).equals(relation.getAttributes().get(k).getName())
								&& commonAttributeType.get(j).equals(relation.getAttributes().get(k).getType())
								&& commonAttributeSize.get(j).equals(relation.getAttributes().get(k).getAttributeSize())) {
							isExist = true;
						}
					}
					if (!isExist) {
						commonAttribute.remove(j);
						commonAttributeType.remove(j);
						commonAttributeSize.remove(j);
						j--;
					}
				}
			}
		}
	}

	void getRestAttribute() {

		restAttribute = new Vector<String>();
		if (tableList.size() >= 1) {
			nickName = Utility.getNickName(tableList.get(0));
		}
		for (int i = 0; i < tableList.size(); i++) {
			relationId = ObjectHolder.getObjectHolder().getRelationId(Utility.getRelationName(tableList.elementAt(i)));
			relation = (Relation) ObjectHolder.getObjectHolder().getObject(relationId);
			for (int j = 0; j < relation.getAttributesCount(); j++) {
				int index = commonAttribute.indexOf(nickName + "." + relation.getAttributes().get(j).getName()) ;
				if (index!=-1 && commonAttributeType.get(index).equals(relation.getAttributes().get(j).getType())
						&& commonAttributeSize.get(index).equals(relation.getAttributes().get(j).getAttributeSize())) {
				}else{
					restAttribute.addElement(Utility.getNickName(tableList.get(i)) + "." + relation.getAttributes().get(j).getName());
				}
			}
		}

	}

	void addCondition(String condition) {
		String prev, cur, prevResult, attributeName;
		prevResult = condition;
		for (int i = 0; i < commonAttribute.size(); i++) {
			attributeName = Utility.getNickName(commonAttribute.get(i));
			prev = Utility.getNickName(tableList.get(0));
			for (int j = 1; j < tableList.size(); j++) {
				cur = Utility.getNickName(tableList.get(j));
				if (prevResult != "" && prevResult != null) {
					prevResult = "( " + prevResult + " and (" + prev + "." + attributeName + " = " + cur + "." + attributeName + ") )";
				} else {
					prevResult = "(" + prev + "." + attributeName + " = " + cur + "." + attributeName + ")";
				}
				prev = cur;
			}
		}
		if (newCondition == null)
			newCondition = "";
		newCondition = prevResult;
	}

	void getProjectionParts(String statement) {
		projectionPart = "";
		String[] tokens = statement.split(",");
		for (int i = 0; i < tokens.length; i++) {
			if (tokens[i].trim().equals("*")) {
				addallTableNames();
			} else {
				if (projectionPart.equals("")) {
					projectionPart = tokens[i];
				} else {
					projectionPart = projectionPart + " , " + tokens[i];
				}

			}
		}
		projectionPart = " " + projectionPart + " ";
	}

	void addallTableNames() {
		for (int i = 0; i < commonAttribute.size(); i++) {
			if (projectionPart.equals("")) {
				projectionPart = commonAttribute.get(i);
			} else {
				projectionPart = projectionPart + " , " + commonAttribute.get(i);
			}
		}
		for (int i = 0; i < restAttribute.size(); i++) {
			if (projectionPart.equals("")) {
				projectionPart = restAttribute.get(i);
			} else {
				projectionPart = projectionPart + " , " + restAttribute.get(i);
			}
		}
	}
}
