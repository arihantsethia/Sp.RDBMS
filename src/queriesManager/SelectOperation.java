package queriesManager;

import java.util.Vector;



public class SelectOperation extends Operation {
    protected Condition condition ;
    protected Projection projection ;
    protected int tableCount ;
    protected Vector<String> tableList ;
    
    
    public SelectOperation(String Statement){
	setType(QueryParser.OperationType.SELECT) ;
	Vector<String> stmtParts = QueryParser.statementParts(Statement,"SELECT") ;
	tableList = QueryParser.getSelectTableList(stmtParts.elementAt(1)) ;
	tableCount = tableList.size() ;
    }
}
