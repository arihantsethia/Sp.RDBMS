package queriesManager;

import java.nio.ByteBuffer;
import java.util.Vector;

import databaseManager.Attribute;
import databaseManager.DatabaseManager;
import databaseManager.DynamicObject;
import databaseManager.Iterator;
import databaseManager.ObjectHolder;
import databaseManager.Relation;
import databaseManager.Utility;

/**
 * 
 * The instance of "UpdateOperation" class is called whenever we want to execute
 * Update query. it also called instance of Condition Class to check Conditions.
 * 
 */
public class UpdateOperation extends Operation {

	protected int tableCount;
	protected String setTuple;
	protected Vector<String> tableList;
	protected Vector<Integer> recordCountList;
	protected Vector<Integer> recordCounterList;
	protected Vector<Iterator> iteratorList;
	protected Vector<DynamicObject> recordObjects;
	protected Relation relation;
	protected long relationId;

	/**
	 * This constructor will be called when we want to create object of class
	 * SelectOperation It takes input query as arguments and split it into
	 * projectionPart , tablePart and conditionPart. It also generates Condition
	 * and Projection class Instance.
	 */

	UpdateOperation(String statement) {
		setType(QueryParser.OperationType.UPDATE);
		recordCountList = new Vector<Integer>();
		recordCounterList = new Vector<Integer>();
		iteratorList = new Vector<Iterator>();
		recordObjects = new Vector<DynamicObject>();
		setCondition(Condition.makeCondition(updateStatementParts(statement)));
	}

	/**
	 * This function divide statement string into three parts 
	 * TableList , setTuple and conditionPart.
	 * @param	statement update query
	 * @return where condition of update query.
	 */
	String updateStatementParts(String statement) {
		int index = statement.indexOf("update");
		statement = statement.substring(index + 6).trim();
		index = statement.indexOf("set");
		tableList = QueryParser.getSelectTableList(statement.substring(0, index));
		tableCount = tableList.size();
		statement = statement.substring(index + 3);
		index = statement.indexOf("where");
		if (index != -1) {
			setTuple = statement.substring(0, index);
			return statement.substring(index + 5);
		}
		setTuple = statement.trim();
		return null;
	}

	/**
	 * This method will execute update query and will reply true if query
	 * successfully executed. It takes records of each table by using iterator
	 * of corresponding class.
	 * IncrementCounter function increments recordCounterList.
	 */
	public boolean executeOperation() {

		for (int i = 0; i < tableList.size(); i++) {
			relationId = ObjectHolder.getObjectHolder().getRelationId(Utility.getRelationName(tableList.elementAt(i)));
			relation = (Relation) ObjectHolder.getObjectHolder().getObject(relationId);
			recordCountList.addElement((int) relation.getRecordsCount());
			recordCounterList.addElement(1);
			iteratorList.addElement(new Iterator(relation));
			recordObjects.addElement(new DynamicObject(relation.getAttributes()));
		}

		for (int i = 0; i < tableList.size(); i++) {
			while (iteratorList.get(i).hasNext()) {
				ByteBuffer a = iteratorList.get(i).getNext();
				if (a != null) {
					recordObjects.set(0, recordObjects.get(i).deserialize(a.array()));
					if (condition == null || condition.compare(recordObjects, tableList)) {
						DynamicObject obj = recordObjects.get(i);
						setValue(setTuple, obj);
						relationId = ObjectHolder.getObjectHolder().getRelationId(Utility.getRelationName(tableList.elementAt(i)));
						relation = (Relation) ObjectHolder.getObjectHolder().getObject(relationId);
						DatabaseManager.getSystemCatalog().updateRecord(relation, iteratorList.get(i).currentPage, (iteratorList.get(i).position - relation.getRecordSize()),
								obj);
					}

				} else {
					break;
				}
			}
		}
		return true;
	}
	
	/**
	 * This function change attributes values of dObject named DynamicObject.  
	 * @param statement setTuple given as argument.
	 * @param dObject DynamicObject of one relation Instance.
	 */
	void setValue(String statement, DynamicObject dObject) {
		String[] token = statement.split(",");
		for (int i = 0; i < token.length; i++) {

			String attributeName = Utility.getNickName(token[i].substring(0, token[i].indexOf("=")).trim());
			String value = token[i].substring(token[i].indexOf("=") + 1).trim();

			for (int j = 0; j < dObject.attributes.size(); j++) {
				if (dObject.attributes.get(j).getName().equals(attributeName) && dObject.attributes.get(j).getType() == Attribute.Type.Char) {
					value = value.replace("\'", "");
					dObject.obj[j] = new String(value);
				} else if (dObject.attributes.get(j).getName().equals(attributeName) && dObject.attributes.get(j).getType() == Attribute.Type.Int) {
					dObject.obj[j] = new Integer(Utility.getUtility().stringToInt(value));
				}
			}
		}
	}
}
