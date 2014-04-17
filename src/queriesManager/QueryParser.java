package queriesManager;

import java.util.Vector;
import java.io.File;

import databaseManager.Attribute;
import databaseManager.Utility;

public class QueryParser {
    public static Attribute attributeName ;
   
    public static enum OperationType {
	JOIN , SELECT , UPDATE ;
	public static String toString(OperationType opType){
	    if(opType==QueryParser.OperationType.JOIN){
		return "JOIN" ;
	    }else if(opType==QueryParser.OperationType.SELECT){
		return "SELECT" ;
	    }else{
		return "UPDATE" ;
	    }
	}
    } ;
    
    public static enum ConditionType {
	OR , AND , SIMPLE , NULL ;
	public static String toString(ConditionType cndType){
	    if(cndType==QueryParser.ConditionType.OR){
		return "OR" ;
	    }else if(cndType==QueryParser.ConditionType.AND){
		return "AND" ;
	    }else{
		return "SIMPLE" ;
	    }
	}
    } ;
    
    public QueryParser(){
    }
    
    
    public boolean isCreateTableQuery(String statement){
	statement = statement.trim();
	String leftPart , rightPart ;
	
	int index = statement.indexOf("table") ;
	if(index!=-1){
	    leftPart = statement.substring(0,index).trim() ;
	    if(!leftPart.trim().equals("create")){
		return false ;
	    }
	    String relationName = statement.substring(index+5,statement.indexOf('(')).trim() ;
	    rightPart = statement.substring(statement.indexOf('(')).trim() ;
	    if(rightPart.charAt(0)=='(' && rightPart.charAt(rightPart.length()-1)==')'){
		    String[] tokens = rightPart.substring(1,rightPart.length()-1).split(",") ;
		    String[][] result = new String[tokens.length][5] ;
		    String[] temp ;
		    String s ;
		    Vector<String> constraints = new Vector<String>() ;
		    constraints.addElement("null") ; constraints.addElement("unique") ;
		    for(int i=0 ; i<tokens.length ;i++){
			temp = tokens[i].trim().split(" ") ;
			if(temp.length>=2 && temp.length <=4){
			    result[i][0] = temp[0].trim() ;
			    result[i][1] = temp[1].trim() ;
			    if(!Utility.isCorrectkeywordType(result[i][1],result[i][2])){
				return false ;
			    }
			    for(int j=2 ;j<temp.length;j++){
				if(constraints.indexOf(temp[j].trim())!=-1){
				    result[i][constraints.indexOf(temp[j].trim())+3] = "1" ;
				}else{
				    return false ;
				}
			    }
			}else{
			    return false ;
			}
		    }
	    }else{
		return false ;
	    }
		return true ;
	}
	return false ;	
    }
    
    static Vector<String> getSelectTableList(String statement){
	Vector<String> result  ;
	statement = statement.trim() ;
	String[] tableList = statement.split(",") ;
	result = new Vector<String>(tableList.length) ;
	for(int i=0 ; i < tableList.length ; i++){
	    result.add(i,tableList[i].trim()) ;
	}
	return result ;	
    }
    
    static Vector<String> statementParts(String statement, String opcode){
	Vector<String> result = new Vector<String>() ;
	statement = statement.toUpperCase() ;
	int index = statement.indexOf(opcode) ;
	statement = statement.substring(index+opcode.length()).trim() ;
	index = statement.indexOf("FROM") ;
	if(index != -1)	{
	    result.addElement(statement.substring(0,index).trim()) ;
	    String restPart = statement.substring(index+4) ;
	    index = restPart.indexOf("WHERE") ;
	    if(index != -1){
		result.addElement(restPart.substring(0,index).trim()) ;
		result.addElement(restPart.substring(index+5,restPart.length()).trim()) ;
	    }else{
		result.addElement(restPart.trim()) ;
	    }	    
	    return result ;
	}
	return null ;
    }
    
    static ConditionType getConditionType(String condition){
	int lp , rp , i ;
	if(condition==null)
	    return ConditionType.NULL ;
	condition = condition.trim().substring(1,condition.length()-1).trim() ;
	if(condition.charAt(0)=='('){
	    lp = 1 ; rp = 0 ;
	    for(i=1;i<condition.length()-1;i++){
		if(condition.charAt(i)==')'){
		    rp++ ;
		}else if(condition.charAt(i)=='('){
		    lp++ ;
		}
		if(lp==rp){
		    break ;
		}
	    }
	    if(condition.substring(i+1,i+1+condition.substring(i+1).indexOf('(')).trim().toUpperCase().equals("AND"))
	    {
		return ConditionType.AND ;
	    }else{
		return ConditionType.OR ;
	    }
	}else{
	    return ConditionType.SIMPLE ;
	} 
    }
 }
