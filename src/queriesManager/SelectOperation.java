package queriesManager;

import java.nio.ByteBuffer;
import java.util.Vector;

import databaseManager.Attribute;
import databaseManager.DynamicObject;
import databaseManager.Iterator;
import databaseManager.ObjectHolder;
import databaseManager.Relation;
import databaseManager.Utility;

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

	public boolean executeOperation() {
		long count = 1;
		for (int i = 0; i < tableList.size(); i++) {
			relationId = ObjectHolder.getObjectHolder().getRelationId(Utility.getRelationName(tableList.elementAt(i)));
			relation = (Relation) ObjectHolder.getObjectHolder().getObject(relationId);
			recordCountList.addElement(relation.getRecordsCount());
			recordCounterList.addElement((long)1);
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
			recordCounterList.set(i, (long)1);
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
