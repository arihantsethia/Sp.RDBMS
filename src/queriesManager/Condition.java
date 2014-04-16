package queriesManager;

import queriesManager.QueryParser.ConditionType;

public abstract class Condition {
    public ConditionType conditionType ;
    
    public static Condition makeCondition(String statement){
	statement = statement.toUpperCase() ;
	return 	null ;
    }
    
}
