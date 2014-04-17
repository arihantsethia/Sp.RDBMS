package queriesManager;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import databaseManager.*;

public class CreateOperation extends Operation {
	
	protected String indexName;
	protected String relationName;
	protected Vector<Vector<String>> parsedData;
	protected int queryType;
	
	CreateOperation(String statement) {
		setType(QueryParser.OperationType.CREATE);
		statement = statement.substring(statement.toUpperCase().indexOf("CREATE") + 6).trim();
		if (statement.toUpperCase().indexOf("TABLE") == 0) {
			queryType = parseCreateTableQuery(statement)?0:-1;
		} else if (statement.toUpperCase().indexOf("INDEX") == 0) {
			queryType = parseCreateIndexQuery(statement)?1:-1;
		}
	}

	private boolean parseCreateTableQuery(String statement) {
		statement = statement.substring(statement.toUpperCase().indexOf("TABLE") + 5).trim();
		relationName = statement.substring(0, statement.indexOf("(")).trim();
		StringTokenizer tokens = new StringTokenizer(statement.substring(statement.indexOf("(") + 1, statement.lastIndexOf(")")), ",");
		Vector<String> parsedToken;
		while (tokens.hasMoreTokens()) {
			parsedToken = new Vector<String>(5);
			StringTokenizer attributeTokens = new StringTokenizer(tokens.nextToken().trim(), " ");
			if (attributeTokens.countTokens() < 2) {
				System.out.println("Name and type of attribute needs to be specified!");
				return false;
			}
			parsedToken.set(0,attributeTokens.nextToken().trim());
			parsedToken.set(1,attributeTokens.nextToken().trim());
			if(Attribute.stringToType(parsedToken.get(1))==Attribute.Type.Undeclared){
				System.out.println("Not a valid data type!");
				return false;
			}else if(Attribute.stringToType(parsedToken.get(1))==Attribute.Type.Char){
				int size = 2;
				if(parsedToken.get(1).indexOf("(")!=-1 && parsedToken.get(1).indexOf(")")!=-1){
					try{
						size = size * Integer.parseInt(parsedToken.get(1).substring(parsedToken.get(1).indexOf("(") + 1, parsedToken.get(1).indexOf(")")).trim());
						parsedToken.set(2,String.valueOf(size));
					}catch (NumberFormatException e){
						System.out.println("Integer length needs to be specified for char data type as \"(length)\" !");
						return false;
					}
				}else{
					System.out.println("Length needs to be specified for char data type as \"(length)\" !");
					return false;
				}
			}else{
				parsedToken.set(2,"4");
			}
			if (attributeTokens.countTokens() < 3) {
				Boolean[] properties = new Boolean[2];
				Arrays.fill(properties, Boolean.FALSE);
				while(attributeTokens.hasMoreTokens()){
					if(attributeTokens.nextToken().equals("notnull") && !properties[0]){
						properties[0] = true;
					}else if(attributeTokens.nextToken().equals("notnull") && !properties[1]){
						properties[1] = true;
					}else{
						System.out.println("Improper Syntax!");
						return false;
					}
				}
				parsedToken.set(3,properties[0].toString());
				parsedToken.set(4,properties[0].toString());				
			} else {
				System.out.println("Only 4 properties per attribute i.e name , type , null/not_null , unique are allowed");
				return false;
			}
		}
		return true;
	}

	private boolean parseCreateIndexQuery(String statement) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public void executeOperation() {
		if(queryType==0){
			
		}else if(queryType==1){
			
		}
	}
}
