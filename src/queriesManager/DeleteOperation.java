package queriesManager;

import java.nio.ByteBuffer;
import java.util.Vector;

import databaseManager.DynamicObject;
import databaseManager.Iterator;
import databaseManager.ObjectHolder;
import databaseManager.Relation;
import databaseManager.Utility;

public class DeleteOperation extends Operation {
	
	protected int tableCount;
	protected String setTuple ;
	protected Vector<String> tableList;
	protected Vector<Integer> recordCountList;
	protected Vector<Integer> recordCounterList;
	protected Vector<Iterator> iteratorList;
	protected Vector<DynamicObject> recordObjects;
	protected Relation relation;
	protected long relationId;
	
	DeleteOperation(String statement){
    	setType(QueryParser.OperationType.DELETE);
    	recordCountList = new Vector<Integer>();
		recordCounterList = new Vector<Integer>();
		iteratorList = new Vector<Iterator>();
		recordObjects = new Vector<DynamicObject>();
		setCondition(Condition.makeCondition(deleteStatementParts(statement))) ;
    }
	
	String deleteStatementParts(String statement){
		int index = statement.indexOf("from");
		statement = statement.substring(index + 4).trim();
		index = statement.indexOf("where") ;
		if(index != -1){
			tableList = QueryParser.getSelectTableList(statement.substring(0,index)) ;
			tableCount = tableList.size();
			return statement.substring(index+5) ;
		}
		tableList = QueryParser.getSelectTableList(statement) ;
		tableCount = tableList.size();
		return null ;
	}
	
	public boolean executeOperation() {


		for (int i = 0; i < tableList.size(); i++) {
			relationId = ObjectHolder.getObjectHolder().getRelationId(Utility.getRelationName(tableList.elementAt(i)));
			relation = (Relation) ObjectHolder.getObjectHolder().getObject(relationId);
			recordCountList.addElement((int) relation.getRecordsCount());
			recordCounterList.addElement(1);
			iteratorList.addElement(new Iterator(relation));
			recordObjects.addElement(new DynamicObject(relation.getAttributes()));
		}

		for(int i=0 ; i<tableList.size() ; i++){
			while(iteratorList.get(i).hasNext()) {
				ByteBuffer a = iteratorList.get(i).getNext();
				if (a != null) {
					recordObjects.set(i, recordObjects.get(i).deserialize(a.array()));
					if (condition == null || condition.compare(recordObjects, tableList)){
						/*
							in this part record has to be delete.
						*/
					}
					
				} else {
					break ;
				}
			}
		}
		return true;
	}
	
	void print() {
		String s = "";
		for (int i = 0; i < tableList.size(); i++) {
			String y = recordObjects.get(i).printRecords();
			s = y + " === " + s;
		}
		System.out.println(s);
	}
}
