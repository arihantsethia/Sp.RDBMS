package queriesManager;

import java.util.Vector;
import java.util.TreeMap;
import java.io.File;

import databaseManager.Attribute;
import databaseManager.ObjectHolder;
import databaseManager.Relation;
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
	OR , AND , SIMPLE ;
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
    
    public boolean isSelectStatementQuery(String statement){
		String stmtUpperCase = statement.toUpperCase().trim();
		int selectIndex = stmtUpperCase.indexOf("SELECT");
		int fromIndex = stmtUpperCase.indexOf("FROM");
		
		if(selectIndex == 0 && fromIndex != -1){
			String selectPart = "",fromPart = "";
			selectPart = stmtUpperCase.substring(selectIndex + 6, fromIndex).trim();
			int whereIndex = stmtUpperCase.indexOf("WHERE");
			
			TreeMap<String,String> tableMap = new TreeMap<String,String>(String.CASE_INSENSITIVE_ORDER);	
			String [] selectPartSplit = selectPart.split(",");
			String key = "",field = "";
			String relationName = "";
			
			
			if(whereIndex != -1){
				String wherePart = "";
				fromPart = stmtUpperCase.substring(fromIndex + 4, whereIndex).trim() ;
				wherePart = stmtUpperCase.substring(whereIndex + 5).trim();
				String [] fromPartSplit = fromPart.split(",");
				
				for(int i=0;i<fromPartSplit.length;i++){
					tableMap.put(Utility.getNickName(fromPartSplit[i]),Utility.getRelationName(fromPartSplit[i]));
				}
				
				for(int j=0;j<selectPartSplit.length;j++){
					key = Utility.getAlias(selectPartSplit[j]);
					if(tableMap.containsKey(key)){
						field = Utility.getFieldName(selectPartSplit[j]);
						relationName = tableMap.get(key);
						long newRelationId = ObjectHolder.getObjectHolder().getRelationIdByRelationName(relationName);
						if (newRelationId != -1) {
						    Relation newRelation = (Relation) ObjectHolder.getObjectHolder().getObject(newRelationId);
						    Vector<Attribute> attributes = newRelation.getAttributes();
						    if(!attributes.contains(field)){
						    	return false;
						    }
						}
						else{
							return false ;
						}
					}
					else{
						return false;
					}
				}
			}
			else{
				fromPart = stmtUpperCase.substring(fromIndex + 4).trim();
				String [] fromPartSplit = fromPart.split(",");
				
				for(int i=0;i<fromPartSplit.length;i++){
					tableMap.put(Utility.getNickName(fromPartSplit[i]),Utility.getRelationName(fromPartSplit[i]));
				}
				
				for(int j=0;j<selectPartSplit.length;j++){
					key = Utility.getAlias(selectPartSplit[j]);
					if(tableMap.containsKey(key)){
						field = Utility.getFieldName(selectPartSplit[j]);
						relationName = tableMap.get(key);
						long newRelationId = ObjectHolder.getObjectHolder().getRelationIdByRelationName(relationName);
						if (newRelationId != -1) {
						    Relation newRelation = (Relation) ObjectHolder.getObjectHolder().getObject(newRelationId);
						    Vector<Attribute> attributes = newRelation.getAttributes();
						    if(!attributes.contains(field)){
						    	return false ;
						    }
						}
						else{
							return false ;
						}
					}
					else{
						return false;
					}
				}
			}
		}
		else{
			return false;
		}
    	return true ;
    }
}
