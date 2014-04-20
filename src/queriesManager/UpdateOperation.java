package queriesManager;

import java.nio.ByteBuffer;
import java.util.Vector;

import databaseManager.DynamicObject;
import databaseManager.Iterator;
import databaseManager.ObjectHolder;
import databaseManager.Relation;
import databaseManager.Utility;

public class UpdateOperation extends Operation {
	
	protected int tableCount;
	protected String setTuple ;
	protected Vector<String> tableList;
	protected Vector<Integer> recordCountList;
	protected Vector<Integer> recordCounterList;
	protected Vector<Iterator> iteratorList;
	protected Vector<DynamicObject> recordObjects;
	protected Relation relation;
	protected long relationId;
	
	
	UpdateOperation(String statement){
    	setType(QueryParser.OperationType.UPDATE);
    	recordCountList = new Vector<Integer>();
		recordCounterList = new Vector<Integer>();
		iteratorList = new Vector<Iterator>();
		recordObjects = new Vector<DynamicObject>();
		setCondition(Condition.makeCondition(updateStatementParts(statement))) ;
    }
	
	String updateStatementParts(String statement){
		int index = statement.indexOf("update") ;
		statement = statement.substring(index + 6).trim();
		index = statement.indexOf("set");
		tableList = QueryParser.getSelectTableList(statement.substring(0,index)) ;
		tableCount = tableList.size();
		statement = statement.substring(index+3) ;
		index = statement.indexOf("where");
		if(index!=-1)
		{
			setTuple = statement.substring(0,index) ;
			return statement.substring(index+5) ;
		}
		return null;
	}
	
	public boolean executeOperation() {


		for (int i = 0; i < tableList.size(); i++) {
			relationId = ObjectHolder.getObjectHolder().getRelationIdByRelationName(Utility.getRelationName(tableList.elementAt(i)));
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
							in this part record has to update.
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
