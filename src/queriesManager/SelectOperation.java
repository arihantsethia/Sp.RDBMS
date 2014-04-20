package queriesManager;

import java.nio.ByteBuffer;
import java.util.Vector;

import databaseManager.DynamicObject;
import databaseManager.Iterator;
import databaseManager.ObjectHolder;
import databaseManager.Relation;
import databaseManager.Utility;

public class SelectOperation extends Operation {
	protected Projection projection;
	protected int tableCount;
	protected Vector<String> tableList;
	protected Vector<Integer> recordCountList;
	protected Vector<Integer> recordCounterList;
	protected Vector<Iterator> iteratorList;
	protected Vector<DynamicObject> recordObjects;
	protected Relation relation;
	protected long relationId;
	
	public SelectOperation(String statement) {
		setType(QueryParser.OperationType.SELECT);
		Vector<String> stmtParts = QueryParser.statementParts(statement, "SELECT");
		tableList = QueryParser.getSelectTableList(stmtParts.elementAt(1));
		tableCount = tableList.size();

		if (stmtParts.size() == 3) {
			setCondition(Condition.makeCondition(stmtParts.elementAt(2)));
		} else {
			setCondition(null);
		}

		recordCountList = new Vector<Integer>();
		recordCounterList = new Vector<Integer>();
		iteratorList = new Vector<Iterator>();
		recordObjects = new Vector<DynamicObject>();
	}

	public boolean executeOperation() {
		long count = 1;
		for (int i = 0; i < tableList.size(); i++) {
			relationId = ObjectHolder.getObjectHolder().getRelationId(Utility.getRelationName(tableList.elementAt(i)));
			relation = (Relation) ObjectHolder.getObjectHolder().getObject(relationId);
			recordCountList.addElement((int) relation.getRecordsCount());
			recordCounterList.addElement(1);
			count = count * recordCountList.get(i);
			iteratorList.addElement(new Iterator(relation));
			recordObjects.addElement(new DynamicObject(relation.getAttributes()));
		}
		if (count != 0) {
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
				print();
			}
			count--;
		}
		while (count > 0) {
			incrementCounter();
			if (condition == null || condition.compare(recordObjects, tableList)){
				print();
			}
			count--;
		}
		return true;
	}

	void incrementCounter() {
		int i = tableList.size() - 1;
		ByteBuffer buffer = null;
		while ((i >= 0) && (recordCounterList.get(i) == recordCountList.get(i))) {
			iteratorList.get(i).initialize();
			while (iteratorList.get(i).hasNext()) {
				buffer = iteratorList.get(i).getNext();
				if (buffer != null) {
					break;
				}
			}
			recordObjects.set(i, recordObjects.get(i).deserialize(buffer.array()));
			recordCounterList.set(i, 1);
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

	void print() {
		String s = "";
		for (int i = 0; i < tableList.size(); i++) {
			String y = recordObjects.get(i).printRecords();
			s = y + " | " + s;
		}
		System.out.println(s);
	}
}
