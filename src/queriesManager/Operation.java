package queriesManager;

import queriesManager.QueryParser.OperationType;

public abstract class Operation {
    protected OperationType operationType ;
    
    void setType(OperationType opType){
	operationType = opType ;
    }
}
