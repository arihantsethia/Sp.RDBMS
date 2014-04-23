package queriesManager;

import java.nio.ByteBuffer;
import java.util.Vector;
import databaseManager.DynamicObject;
import databaseManager.Iterator;
import databaseManager.ObjectHolder;
import databaseManager.Relation;
import databaseManager.Utility;

/**
 * 
 * The instance of "SelectOperation" class is called whenever we want to execute
 * Select query. it also called instance of Condition Class to check Conditions.
 * 
 */
public class SelectOperation extends Operation {
	protected Projection projection;
	protected int tableCount;
	protected Vector<String> tableList;
	protected Vector<Long> recordCountList;
	protected Vector<Long> recordCounterList;
	protected Vector<Iterator> iteratorList;
	protected Vector<DynamicObject> recordObjects;
	protected Relation relation;
	protected long relationId;
	protected String projectionPart;

	int p = 0;

	/**
	 * This constructor will be called when we want to create object of class
	 * SelectOperation It takes input query as arguments and split it into
	 * projectionPart , tablePart and conditionPart. It also generates Condition
	 * and Projection class Instance.
	 */
	public SelectOperation(String statement) {
		setType(QueryParser.OperationType.SELECT);
		Vector<String> stmtParts = QueryParser.statementParts(statement, "select");
		projectionPart = stmtParts.elementAt(0);
		tableList = QueryParser.getSelectTableList(stmtParts.elementAt(1));
		tableCount = tableList.size();

		if (stmtParts.size() == 3) {
			setCondition(Condition.makeCondition(stmtParts.elementAt(2)));
		} else {
			setCondition(null);
		}
		projection = new Projection();
		recordCountList = new Vector<Long>();
		recordCounterList = new Vector<Long>();
		iteratorList = new Vector<Iterator>();
		recordObjects = new Vector<DynamicObject>();
	}

	/**
	 * This method will execute select query and will reply true if query
	 * successfully executed. It takes records of each table by using iterator
	 * of corresponding class IncrementCounter function increment
	 * recordCounterList.
	 */
	@Override
	public boolean executeOperation() {
		long count = 1;
		for (int i = 0; i < tableList.size(); i++) {
			relationId = ObjectHolder.getObjectHolder().getRelationId(Utility.getRelationName(tableList.elementAt(i)));
			relation = (Relation) ObjectHolder.getObjectHolder().getObject(relationId);
			recordCountList.addElement(relation.getRecordsCount());
			recordCounterList.addElement((long) 1);
			count = count * recordCountList.get(i);
			iteratorList.addElement(new Iterator(relation));
			recordObjects.addElement(new DynamicObject(relation.getAttributes()));
		}
		if (count != 0) {
			projection.printTableAttributes(projectionPart, tableList, recordObjects);
			for (int i = 0; i < tableList.size(); i++) {
				if (iteratorList.get(i).hasNext()) {
					ByteBuffer a = iteratorList.get(i).getNext();
					if (a != null) {
						recordObjects.set(i, recordObjects.get(i).deserialize(a.array()));
					} else {
						i--;
					}
				}
			}
			if (condition == null || condition.compare(recordObjects, tableList)) {
				projection.printRecords(projectionPart, tableList, recordObjects);
			}
			count--;
		}
		while (count > 0) {
			incrementCounter();
			if (condition == null || condition.compare(recordObjects, tableList)) {
				projection.printRecords(projectionPart, tableList, recordObjects);
			}
			count--;
		}
		return true;
	}

	/**
	 * IncrementCounter function increment recordCounterList. Suppose total no.
	 * of records corresponding to tables (a,b,c) are (1,3,2) then The sequence
	 * of records retrieval will be like that :- (1,1,1) -> (1,1,2) -> (1,2,1)
	 * -> (1,2,2) -> (1,3,1) -> (1,3,2).
	 */
	void incrementCounter() {
		int i = tableList.size() - 1;
		ByteBuffer buffer = null;
		while ((i >= 0) && (recordCounterList.get(i).equals(recordCountList.get(i)))) {
			iteratorList.get(i).initialize();
			while (iteratorList.get(i).hasNext()) {
				buffer = iteratorList.get(i).getNext();
				if (buffer != null) {
					break;
				}
			}
			recordObjects.set(i, recordObjects.get(i).deserialize(buffer.array()));
			recordCounterList.set(i, (long) 1);
			i--;
		}
		if (i >= 0) {
			while (iteratorList.get(i).hasNext()) {
				buffer = iteratorList.get(i).getNext();
				if (buffer != null) {
					break;
				}
			}
			recordObjects.set(i, recordObjects.get(i).deserialize(buffer.array()));
			recordCounterList.set(i, recordCounterList.get(i) + 1);
		}
	}

}
