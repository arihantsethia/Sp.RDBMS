package queriesManager;

import java.nio.ByteBuffer;
import java.util.Vector;

import databaseManager.DatabaseManager;
import databaseManager.DynamicObject;
import databaseManager.Index;
import databaseManager.Iterator;
import databaseManager.ObjectHolder;
import databaseManager.Relation;
import databaseManager.Utility;

/**
 * 
 * The instance of "DeleteOperation" class is called whenever we want to Delete
 * a query. it also calls instance of Condition Class to check associates
 * conditions Evaluate with it.
 * 
 */
public class DeleteOperation extends Operation {

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
	 * DeleteOperation It takes input query as arguments and split it into
	 * projectionPart , tablePart and conditionPart. It also generates Condition
	 * and Projection class Instance if Necessary.
	 */

	DeleteOperation(String statement) {
		setType(QueryParser.OperationType.DELETE);
		recordCountList = new Vector<Integer>();
		recordCounterList = new Vector<Integer>();
		iteratorList = new Vector<Iterator>();
		recordObjects = new Vector<DynamicObject>();
		setCondition(Condition.makeCondition(deleteStatementParts(statement)));
	}

	/**
	 * This function divide statement string into three parts TableList ,
	 * setTuple and conditionPart.
	 * 
	 * @param statement
	 *            delete query
	 * @return where condition of delete query.
	 */
	String deleteStatementParts(String statement) {
		int index = statement.indexOf("from");
		statement = statement.substring(index + 4).trim();
		index = statement.indexOf("where");
		if (index != -1) {
			tableList = QueryParser.getSelectTableList(statement.substring(0, index));
			tableCount = tableList.size();
			return statement.substring(index + 5);
		}
		tableList = QueryParser.getSelectTableList(statement);
		tableCount = tableList.size();
		return null;
	}

	/**
	 * This method will execute delete query and will reply true if query
	 * successfully executed. It takes records of each table by using iterator
	 * of corresponding class. IncrementCounter function increment
	 * recordCounterList.
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
					recordObjects.set(i, recordObjects.get(i).deserialize(a.array()));
					if (condition == null || condition.compare(recordObjects, tableList)) {
						relationId = ObjectHolder.getObjectHolder().getRelationId(Utility.getRelationName(tableList.elementAt(i)));
						relation = (Relation) ObjectHolder.getObjectHolder().getObject(relationId);
						int recordOffset = iteratorList.get(i).position - relation.getRecordSize();
						DatabaseManager.getSystemCatalog().deleteRecord(relationId, iteratorList.get(i).currentPage, recordOffset,recordObjects.get(i));
					}
				} else {
					break;
				}
			}
		}
		return true;
	}
}
