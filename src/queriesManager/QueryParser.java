package queriesManager;

import java.util.Vector;
import java.util.TreeMap;
import java.io.File;

import databaseManager.Attribute;
import databaseManager.ObjectHolder;
import databaseManager.Relation;
import databaseManager.Utility;
import databaseManager.Attribute.Type;

public class QueryParser {
	public static Attribute attributeName;
	public static TreeMap<String,String> tableMap;
	
	public static enum OperationType {
		JOIN, SELECT, UPDATE;
		public static String toString(OperationType opType) {
			if (opType == QueryParser.OperationType.JOIN) {
				return "JOIN";
			} else if (opType == QueryParser.OperationType.SELECT) {
				return "SELECT";
			} else {
				return "UPDATE";
			}
		}
	};

	public static enum ConditionType {
		OR, AND, SIMPLE, NULL;
		public static String toString(ConditionType cndType) {
			if (cndType == QueryParser.ConditionType.OR) {
				return "OR";
			} else if (cndType == QueryParser.ConditionType.AND) {
				return "AND";
			} else {
				return "SIMPLE";
			}
		}
	};

	public static enum OperatorType {
		LESS, GREATER, LESSEQUAL, GREATEQUAL, EQUAL;
		public static String toString(OperatorType opType) {
			if (opType == QueryParser.OperatorType.LESS) {
				return "<";
			} else if (opType == QueryParser.OperatorType.GREATER) {
				return ">";
			}else if (opType == QueryParser.OperatorType.LESSEQUAL) {
				return "<=";
			} else if (opType == QueryParser.OperatorType.GREATEQUAL) {
				return ">=";
			} else {
				return "=";
			}
		}
	};

	public static enum DataType {
		Int, Long, Boolean, Float, Double, Char, Undeclared;
		public static Type toType(int value) {
			return Type.values()[value];
		}

		public static int toInt(Type value) {
			return value.ordinal();
		}

	};

	public QueryParser() {
		tableMap = new TreeMap<String,String>(String.CASE_INSENSITIVE_ORDER);
	}
	

	public boolean isCreateTableQuery(String statement) {
		statement = statement.trim();
		String leftPart, rightPart;

		int index = statement.indexOf("table");
		if (index != -1) {
			leftPart = statement.substring(0, index).trim();
			if (!leftPart.trim().equals("create")) {
				return false;
			}
			String relationName = statement.substring(index + 5, statement.indexOf('(')).trim();
			rightPart = statement.substring(statement.indexOf('(')).trim();
			if (rightPart.charAt(0) == '(' && rightPart.charAt(rightPart.length() - 1) == ')') {
				String[] tokens = rightPart.substring(1, rightPart.length() - 1).split(",");
				String[][] result = new String[tokens.length][5];
				String[] temp;
				String s;
				Vector<String> constraints = new Vector<String>();
				constraints.addElement("null");
				constraints.addElement("unique");
				for (int i = 0; i < tokens.length; i++) {
					temp = tokens[i].trim().split(" ");
					if (temp.length >= 2 && temp.length <= 4) {
						result[i][0] = temp[0].trim();
						result[i][1] = temp[1].trim();
						if (!Utility.isCorrectkeywordType(result[i][1], result[i][2])) {
							return false;
						}
						for (int j = 2; j < temp.length; j++) {
							if (constraints.indexOf(temp[j].trim()) != -1) {
								result[i][constraints.indexOf(temp[j].trim()) + 3] = "1";
							} else {
								return false;
							}
						}
					} else {
						return false;
					}
				}
			} else {
				return false;
			}
			return true;
		}
		return false;
	}

	static Vector<String> getSelectTableList(String statement) {
		Vector<String> result;
		statement = statement.trim();
		String[] tableList = statement.split(",");
		result = new Vector<String>(tableList.length);
		for (int i = 0; i < tableList.length; i++) {
			result.add(i, tableList[i].trim());
		}
		return result;
	}

	static Vector<String> statementParts(String statement, String opcode) {
		Vector<String> result = new Vector<String>();
		int index = statement.indexOf(opcode);
		statement = statement.substring(index + opcode.length()).trim();
		index = statement.indexOf("from");
		if (index != -1) {
			result.addElement(statement.substring(0, index).trim());
			String restPart = statement.substring(index + 4);
			index = restPart.indexOf("where");
			if (index != -1) {
				result.addElement(restPart.substring(0, index).trim());
				result.addElement(restPart.substring(index + 5, restPart.length()).trim());
			} else {
				result.addElement(restPart.trim());
			}
			return result;
		}
		return null;
	}

    public boolean isUpdateStatementQuery(String statement){
    	String stmtUpperCase = statement.toUpperCase().trim();
    	int updateIndex = stmtUpperCase.indexOf("UPDATE");
    	int setIndex = stmtUpperCase.indexOf("SET");
    	int whereIndex = stmtUpperCase.indexOf("WHERE");
    	
    	if(updateIndex == 0 && setIndex != -1 && whereIndex != -1){
    		String updatePart = "", setPart = "", wherePart = "";
    		updatePart = stmtUpperCase.substring(updateIndex + 6, setIndex).trim();
    		setPart = stmtUpperCase.substring(setIndex + 3, whereIndex).trim();
    		wherePart = stmtUpperCase.substring(whereIndex + 5).trim();
    	}
    	else{
    		return false;
    	}
    	return true;
    }
    
    public boolean isSelectStatementQuery(String statement){
		String stmtUpperCase = statement.toUpperCase().trim();
		int selectIndex = stmtUpperCase.indexOf("SELECT");
		int fromIndex = stmtUpperCase.indexOf("FROM");
		
		if(selectIndex == 0 && fromIndex != -1){
			String selectPart = "",fromPart = "";
			selectPart = stmtUpperCase.substring(selectIndex + 6, fromIndex).trim();
			int whereIndex = stmtUpperCase.indexOf("WHERE");
			
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
				
				boolean whereClause = isCondition(wherePart);
				
				if(whereClause){
					return true;
				}
				else{
					return false;
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

    static ConditionType getConditionType(String condition){
    	int lp , rp , i ;
		if(condition==null){
			return ConditionType.NULL ;
		}
		condition = condition.trim().substring(1,condition.length()-1).trim() ;
		if(condition.charAt(0)=='('){
		    lp = 1 ; rp = 0 ;
		    for(i=1;i<condition.length()-1;i++){
				if(condition.charAt(i)==')'){
				    rp++ ;
				}
				else if(condition.charAt(i)=='('){
				    lp++ ;
				}
				if(lp==rp){
				    break ;
				}
		    }
		    if(condition.substring(i+1,i+1+condition.substring(i+1).indexOf('(')).trim().toUpperCase().equals("AND"))
		    {
		    	return ConditionType.AND ;
		    }
		    else{
		    	return ConditionType.OR ;
		    }
		}
		else{
		    	return ConditionType.SIMPLE ;
		} 
    }   

	static boolean isCondition(String s){
	    System.out.println(s) ;
	    s = s.trim() ;
	    String firstPart , lastPart ;
	    Vector<String> logicalOp , arithmeticOp ;
	    logicalOp = new Vector<String>() ;
	    arithmeticOp = new Vector<String>() ;
	    logicalOp.addElement("AND") ; logicalOp.addElement("OR") ;   
	    arithmeticOp.addElement("<=") ; arithmeticOp.addElement(">=") ; arithmeticOp.addElement("<") ; arithmeticOp.addElement(">") ;
	    arithmeticOp.addElement("=") ;
	    
	    if(s.charAt(0)=='(' && s.charAt(s.length()-1)==')'){
	    	s  = s.substring(1,s.length()-1).trim() ;
			if(s.charAt(0)=='(' && s.charAt(s.length()-1)==')'){
			    int lp = 1 , rp = 0 , i ;
			    for(i=1;i<s.length()-1;i++){
			    	if(s.charAt(i)==')'){
			    		rp++ ;
			    	}
			    	else if(s.charAt(i)=='('){
			    		lp++ ;
			    	}
			    	if(lp==rp){
			    		break ;
			    	}
			    }
			    if(i<s.length()-1 && logicalOp.contains(s.substring(i+1,i+1+s.substring(i+1).indexOf('(')).trim().toUpperCase())){
			    	return isCondition(s.substring(0,i+1)) && isCondition(s.substring(i+1 + s.substring(i+1).indexOf('('))) ;
			    }
			}
			else if(!s.contains("(") && !s.contains(")")){
			    for(int j=0 ; j<arithmeticOp.size();j++){
			    	if(s.toUpperCase().indexOf(arithmeticOp.get(j)) != -1){
			    		firstPart = s.substring(0,s.toUpperCase().indexOf(arithmeticOp.get(j))).trim() ;
			    		lastPart = s.substring(s.toUpperCase().indexOf(arithmeticOp.get(j))+arithmeticOp.get(j).length()).trim()  ;
			    		System.out.println(firstPart + "===" + lastPart) ;
			    		/*
				  			first part check number , string , variable(s.id)
			    		 */
			    		if(Utility.isVariable(firstPart) && Utility.isVariable(lastPart)){
			    			if(Utility.checkType(firstPart,lastPart)){
			    				return true;
			    			}
			    			else{
			    				return false;
			    			}
			    		}
			    		else if(Utility.isVariable(firstPart)){
			    			if(Utility.isSameType("int",lastPart)){
			    				return true ;
			    			}
			    			else if(Utility.isString(firstPart)){
			    				return true ;
			    			}
			    		}
			    		else if(Utility.isVariable(lastPart)){
			    			if(Utility.isNum(firstPart)){
			    				// call to function to check type of firstPart and lastPart
			    			}
			    			else if(Utility.isString(firstPart)){
			    				// call to function to check type of firstPart and lastPart
			    			}
			    		}
			    		
			    		return true ;
			    	}
			    }
			}
	    }
	    return false ;
	}

	static OperatorType getOperatorType(String statement) {
		if (statement.contains("<=")) {
			return OperatorType.LESSEQUAL;
		} else if (statement.contains(">=")) {
			return OperatorType.GREATEQUAL;
		} else if (statement.contains("<")) {
			return OperatorType.LESS;
		} else if (statement.contains(">")) {
			return OperatorType.GREATER;
		} else {
			return OperatorType.EQUAL;
		}
	}

	static String getLeftOperand(String statement, OperatorType opType) {
		String operator = OperatorType.toString(opType);		
		int index = statement.indexOf(operator);
		return statement.substring(0, index).trim();
	}

	static String getRightOperand(String statement, OperatorType opType) {
		String operator = OperatorType.toString(opType);
		int index = statement.indexOf(operator);
		return statement.substring(index + operator.length()).trim();
	}
}

