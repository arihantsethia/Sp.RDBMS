package queriesManager;

import queriesManager.QueryParser.OperationType;

public abstract class Operation {
    protected OperationType operationType ;
    protected Condition condition ;
    
    void setType(OperationType opType){
	operationType = opType ;
    }
    
    public static Operation makeOperation(String statement){
	statement = statement.toUpperCase() ;
	if(statement.contains("SELECT")){
	   return new  SelectOperation(statement) ;
	}else if(statement.contains("UPDATE")){
	   return new  UpdateOperation(statement) ;
	}else if(statement.contains("INSERT")){
	   return new  InsertOperation(statement) ;	   
	}
	return null;
    }
    
    void setCondition(Condition cond){
	condition = cond ;
    }
}
