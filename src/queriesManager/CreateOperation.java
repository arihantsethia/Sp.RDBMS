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
			queryType = parseCreateTableQuery(statement) ? 0 : -1;
		} else if (statement.toUpperCase().indexOf("INDEX") == 0) {
			queryType = parseCreateIndexQuery(statement) ? 1 : -1;
		}
	}

	private boolean parseCreateTableQuery(String statement) {
		int tableIndex = statement.indexOf("table");
		
		statement = statement.substring(tableIndex + 5).trim();
		
		if(statement.length() == 0){
			System.out.println("table name is missing");
			return false;
		}
		
		//System.out.println(statement);
		
		int lpIndex = statement.indexOf("(");
		int rpIndex = statement.lastIndexOf(")");
		if(lpIndex == -1 || rpIndex != statement.length() -1){
			System.out.println("parenthesis are missing");
			return false;
		}
		
		relationName = statement.substring(0, statement.indexOf("(")).trim();

		if(relationName.contains(" ")){
			System.out.println("Rubbish between table name and opening parenthesis");
			return false ;
		}
		//System.out.println(statement);
		statement = statement.substring(statement.indexOf(relationName) + relationName.length()).trim();
		//System.out.println(statement);
		
		StringTokenizer tokens = new StringTokenizer(statement.substring(statement.indexOf("(") + 1, statement.lastIndexOf(")")).trim(), ",");
		//System.out.println(tokens.countTokens());
		if(tokens.countTokens() == 0){
			System.out.println("Attributes are missing");
			return false;
		}
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
			if (Attribute.stringToType(parsedToken.get(1)) == Attribute.Type.Undeclared) {
				System.out.println("Not a valid data type!");
				return false;
			} 
			else if (Attribute.stringToType(parsedToken.get(1)) == Attribute.Type.Char) {
				int size = 2;
				if (parsedToken.get(1).indexOf("(") != -1 && parsedToken.get(1).indexOf(")") != -1) {
				try {
					String temp = parsedToken.get(1).substring(parsedToken.get(1).indexOf("(") + 1, parsedToken.get(1).indexOf(")")).trim();
					size = size * Integer.parseInt(temp);
					parsedToken.add(String.valueOf(size));
				} catch (NumberFormatException e) {
					System.out.println("Integer length needs to be specified for char data type as \"(length)\" !");
					return false;
				}
			}
				else {
					System.out.println("Length needs to be specified for char data type as \"(length)\" !");
					return false;
				}
			} 
			else {
				parsedToken.add("4");
		}
		if (attributeTokens.countTokens() < 3) {
			Boolean[] properties = new Boolean[2];
			Arrays.fill(properties, Boolean.FALSE);
			while (attributeTokens.hasMoreTokens()) {
				String token = attributeTokens.nextToken();
				if (token.equals("notnull") && !properties[0]) {
					properties[0] = true;
				} 
				else if (token.equals("unique") && !properties[1]) {
					properties[1] = true;
				} 
				else {
					System.out.println("Improper Syntax!");
					return false;
				}
			}
			parsedToken.add(properties[0].toString());
			parsedToken.add(properties[1].toString());
			} 
			else {
				System.out.println("Only 4 properties per attribute i.e name , type , null/not_null , unique are allowed");
				return false;
			}
			parsedData.add(parsedToken);
		}
		return true;
	}

	private boolean parsePrimaryKeyQuery(String statement){
		int createIndex = statement.trim().indexOf("create");
		int primarykeyIndex = statement.trim().indexOf("primarykey");
		int onIndex = statement.trim().indexOf("on");
		int tableIndex = statement.trim().indexOf("table");
		
		if(createIndex == 0 && primarykeyIndex != -1 && onIndex != -1 && tableIndex != -1){
			if(primarykeyIndex <= onIndex && onIndex <= tableIndex){
				String relationName = statement.substring(tableIndex + 5,statement.indexOf("(")).trim();
				long newRelationId = ObjectHolder.getObjectHolder().getRelationId(relationName);
				
				if(newRelationId != -1){
					String primarykeyPart = "";
					primarykeyPart = statement.substring(statement.indexOf("(") + 1,statement.indexOf(")")).trim();
					
					if(primarykeyPart.length() != 0){
						String [] primarykeyPartSplit = primarykeyPart.split(",");
						Relation newRelation = (Relation) ObjectHolder.getObjectHolder().getObject(newRelationId);
					    Vector<Attribute> attributes = newRelation.getAttributes();
					    
					    for(int i=0;i<primarykeyPartSplit.length;i++){
					    	boolean chk = true ;
						    for(int k = 0 ; k < attributes.size() ; k++ ){
						    	if(attributes.get(k).getName().equals(primarykeyPartSplit[i])){
						    		chk = false ;
						    	}
						    }
						    if(chk){
						    	return false ;
						    }
					    }
					}
				}
			}
		}
		return true;
	}
	
	private boolean parseCreateIndexQuery(String statement) {
		statement = statement.substring(statement.toUpperCase().indexOf("INDEX") + 5).trim();
		Vector<String> parsedToken;
		String attribtueName;
		if (statement.contains(" ")) {
			indexName = statement.substring(0, statement.indexOf(" ")).trim();
			statement = statement.substring(statement.indexOf(" ")).trim();
			if(statement.toUpperCase().indexOf("ON") == 0){
				statement = statement.substring(statement.toUpperCase().indexOf("ON") + 2).trim();
				if (statement.toUpperCase().indexOf("TABLE") == 0) {
					statement = statement.substring(statement.toUpperCase().indexOf("TABLE") + 5).trim();
					if(statement.indexOf("(")!=-1){
						relationName = statement.substring(0, statement.indexOf("(")).trim();
						parsedToken = new Vector<String>();
						StringTokenizer attribtueNames = new StringTokenizer(statement.substring(statement.indexOf("(") + 1, statement.lastIndexOf(")")), ",");
						if(attribtueNames.countTokens() > 0){
							while(attribtueNames.hasMoreTokens()){
								attribtueName = attribtueNames.nextToken().trim();
								if(attribtueName.indexOf(" ")==-1){
									parsedToken.add(attribtueName);
								}else{
									System.out.println("All attributes must be seperated by ','. Error near : "+attribtueName);
									return false;
								}
							}
							parsedData.add(parsedToken);
							parsedToken = new Vector<String>();
							statement = statement.substring(statement.lastIndexOf(")")+1).trim();
							if(statement.equalsIgnoreCase("UNIQUE")){
								parsedToken.add("true");
							}else if(statement.length()==0){
								parsedToken.add("false");
							}else{
								System.out.println("Improper syntax. Error near : "+statement);
								return false;
							}
							parsedData.add(parsedToken);
						}else{
							System.out.println("Atleat one attribute should be specified. Error near : "+statement);
							return false;
						}
					}
				}else{
					System.out.println("Error near syntax \"" + statement + "\"");
					return false;
				}
			} else {
				System.out.println("Error near syntax \"" + statement + "\"");
				return false;
			}
		} else {
			System.out.println("Error near syntax \"" + statement + "\"");
			return false;
		}
		return true;
	}

	public boolean executeOperation() {
		if (queryType == 0) {
			return DatabaseManager.getSystemCatalog().createTable(relationName, parsedData);
		} else if (queryType == 1) {
			return DatabaseManager.getSystemCatalog().createIndex(indexName, relationName, parsedData);
		}
		return false;
	}
}
