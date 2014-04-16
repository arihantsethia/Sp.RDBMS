package queriesManager;

import java.nio.ByteBuffer;
import java.util.Vector;

import databaseManager.Iterator;
import databaseManager.ObjectHolder;
import databaseManager.Relation;
import databaseManager.Utility;



public class SelectOperation extends Operation {
    protected Projection projection ;
    protected int tableCount ;
    protected Vector<String> tableList ;
    protected Vector<Integer> recordCountList ;
    protected Vector<Integer> recordCounterList ;
    protected Vector<Iterator> IteratorList ;
    protected Vector<ByteBuffer> dataRecord ;
    protected Relation relation ;
    protected long relationId ;
    
    public SelectOperation(String Statement){
	setType(QueryParser.OperationType.SELECT) ;
	Vector<String> stmtParts = QueryParser.statementParts(Statement,"SELECT") ;
	tableList = QueryParser.getSelectTableList(stmtParts.elementAt(1)) ;
	tableCount = tableList.size() ;
	if(stmtParts.size() == 3){
	    setCondition(Condition.makeCondition(stmtParts.elementAt(1))) ;
	}else{
	    setCondition(null) ;
	}
    }
    
    public void ExecuteOperation(){
	
	long count = 1 ;
	
	for(int i=0 ; i < tableList.size() ; i++){
	    relationId = ObjectHolder.getObjectHolder().getRelationIdByRelationName(Utility.getRelationName(tableList.elementAt(i))) ;
 	    relation   = (Relation) ObjectHolder.getObjectHolder().getObject(relationId) ;
 	    recordCountList.addElement(relation.getRecordNumber()) ;
 	    recordCounterList.addElement(1) ;
 	    count = count * recordCountList.get(i) ;
 	    IteratorList.addElement(new Iterator(relation)) ;
	}
	count-- ;
	while(count>0){
	    for(int i=0 ; i<tableList.size() ;i++ ){
		if(IteratorList.get(i).hasNext()){
		    
		}
	    }
	    
	    IncrementCounter() ;
	}
    }
    
    void IncrementCounter(){
	int i = tableList.size() - 1 ;
	while(recordCounterList.get(i)==recordCountList.get(i)){
	    recordCounterList.add(i,1) ;
	    i-- ;
	}
	recordCounterList.add(i,recordCounterList.get(i)+1) ;
    }
}
