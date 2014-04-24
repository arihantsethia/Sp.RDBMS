package queriesManager;

import java.util.Vector;
import databaseManager.Attribute;
import databaseManager.ObjectHolder;
import databaseManager.Relation;
import databaseManager.Utility;

/**
 * 
 * The instance of "EquiOperation" class is called whenever we want to execute
 * Equip Join query. it calls instance of Select Class to evaluate final
 * expression.
 * 
 */
public class EquiJoin extends Operation {
	protected Projection projection;
	protected int tableCount;
	protected int queryType;
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
	 * EquiJoinOperation It takes input query as arguments and split it into
	 * projectionPart , tablePart and newCondition.
	 */
	public EquiJoin(String statement) {
		setType(QueryParser.OperationType.EQUIJOIN);
		Vector<String> stmtParts = QueryParser.statementParts(statement, "select");
		projectionPart = stmtParts.elementAt(0);
		tablePart = stmtParts.elementAt(1).replace("join", ",").trim();
		tablePart = tablePart.replace("equi", "");
		tableList = QueryParser.getJoinTableList(stmtParts.elementAt(1), "equi");
		tableCount = tableList.size();

		this.getCommonAttribute();
		this.getRestAttribute();

		if (stmtParts.size() == 3) {
			this.addCondition(stmtParts.elementAt(2));
		} else {
			this.addCondition(null);
		}

		projection = new Projection();
	}

	/**
	 * It create intermediate select query to evaluate operation and Execute
	 * operation of that new Intermediate Class.
	 */
	public boolean executeOperation() {
		if(queryType!=-1){
			String fQuery = "";
			if (newCondition == null || newCondition.equals("")) {
				fQuery = "select " + projectionPart + " from " + tablePart;
			} else {
				fQuery = "select " + projectionPart + " from " + tablePart + " where " + newCondition;
			}
			if (QueryParser.isSelectStatementQuery(fQuery)) {
				intermediateOP = Operation.makeOperation(fQuery);
				if (intermediateOP.executeOperation()) {
					return true;
				} else {
					return false;
				}
			} else {
				return false;
			}
		}else{
			System.out.println("Error: Table doesn't exist");
			return false;
		}
	}

	/**
	 * It updates commonAttribute Vector which contains name of attributes which
	 * are common in all relations that are used.
	 */
	void getCommonAttribute() {

		commonAttribute = new Vector<String>();
		commonAttributeType = new Vector<Attribute.Type>();
		commonAttributeSize = new Vector<Integer>();

		for (int i = 0; i < tableList.size(); i++) {

			nickName = Utility.getNickName(tableList.get(i));
			relationId = ObjectHolder.getObjectHolder().getRelationId(Utility.getRelationName(tableList.elementAt(i)));
			if (relationId != -1) {
				relation = (Relation) ObjectHolder.getObjectHolder().getObject(relationId);

				if (i == 0) {
					for (int j = 0; j < relation.getAttributesCount(); j++) {
						commonAttribute.addElement(nickName + "." + relation.getAttributes().get(j).getName());
						commonAttributeType.addElement(relation.getAttributes().get(j).getType());
						commonAttributeSize.addElement(relation.getAttributes().get(j).getAttributeSize());
					}
				}

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
						j--;
					}
				}
			} else {
				queryType = -1;
				return;
			}
		}
	}

	/**
	 * It updates restAttribute Vector which contains name of attributes which
	 * are not in commonAttributes Vector and Contain in any one of relations.
	 */
	void getRestAttribute() {

		restAttribute = new Vector<String>();

		for (int i = 0; i < tableList.size(); i++) {

			nickName = Utility.getNickName(tableList.get(i));
			relationId = ObjectHolder.getObjectHolder().getRelationId(Utility.getRelationName(tableList.elementAt(i)));
			if (relationId != -1) {
				relation = (Relation) ObjectHolder.getObjectHolder().getObject(relationId);
				for (int j = 0; j < relation.getAttributesCount(); j++) {
					if (!commonAttribute.contains(nickName + "." + relation.getAttributes().get(j).getName())) {
						restAttribute.addElement(nickName + "." + relation.getAttributes().get(j).getName());
					}
				}
			} else {
				queryType = -1;
				return;
			}
		}

	}

	/**
	 * It adds extra equality conditions to previous condition to evaluate
	 * expressions.
	 * 
	 * @param condition
	 */
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
};
