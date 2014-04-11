package queriesManager;

import java.util.Vector;
import java.io.File;

import databaseManager.Attribute;
import databaseManager.Utility;

public class QueryParser {
    static Attribute attributeName ;
    
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
}
