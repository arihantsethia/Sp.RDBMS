package queriesManager;

import java.util.Arrays;
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
		parsedData = new Vector<Vector<String>>();
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
			parsedToken = new Vector<String>();
			StringTokenizer attributeTokens = new StringTokenizer(tokens.nextToken().trim(), " ");
			if (attributeTokens.countTokens() < 2) {
				System.out.println("Name and type of attribute needs to be specified!");
				return false;
			}
			parsedToken.add(attributeTokens.nextToken().trim());
			parsedToken.add(attributeTokens.nextToken().trim());
			if(Attribute.stringToType(parsedToken.get(1))==Attribute.Type.Undeclared){
				System.out.println("Not a valid data type!");
				return false;
			}else if(Attribute.stringToType(parsedToken.get(1))==Attribute.Type.Char){
				int size = 2;
				if(parsedToken.get(1).indexOf("(")!=-1 && parsedToken.get(1).indexOf(")")!=-1){
					try{
						String temp = parsedToken.get(1).substring(parsedToken.get(1).indexOf("(") + 1, parsedToken.get(1).indexOf(")")).trim();
						size = size * Integer.parseInt(temp);
						parsedToken.add(String.valueOf(size));
					}catch (NumberFormatException e){
						System.out.println("Integer length needs to be specified for char data type as \"(length)\" !");
						return false;
					}
				}else{
					System.out.println("Length needs to be specified for char data type as \"(length)\" !");
					return false;
				}
			}else{
				parsedToken.add("4");
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
				parsedToken.add(properties[0].toString());
				parsedToken.add(properties[0].toString());		
			} else {
				System.out.println("Only 4 properties per attribute i.e name , type , null/not_null , unique are allowed");
				return false;
			}
			parsedData.add(parsedToken);
		}
		return true;
	}

	private boolean parseCreateIndexQuery(String statement) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public boolean executeOperation() {
		if(queryType==0){
			return DatabaseManager.getSystemCatalog().createTable(relationName, parsedData);
		}else if(queryType==1){
			return DatabaseManager.getSystemCatalog().createIndex(relationName, parsedData);
		}
		return false;
	}
}
