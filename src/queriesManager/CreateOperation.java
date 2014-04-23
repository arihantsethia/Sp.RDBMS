package queriesManager;

import java.util.Arrays;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;

import databaseManager.*;

public class CreateOperation extends Operation {

	protected String indexName;
	protected String relationName;
	protected String dbName;
	protected Vector<Vector<String>> parsedData;
	protected int queryType;

	CreateOperation(String statement) {
		setType(QueryParser.OperationType.CREATE);
		parsedData = new Vector<Vector<String>>();
		queryType = -1;
		int createIndex = statement.trim().indexOf("create");
		if (createIndex == 0) {
			statement = statement.substring(statement.toUpperCase().indexOf("CREATE") + 6).trim();
			if (statement.toUpperCase().indexOf("TABLE") == 0) {
				queryType = parseCreateTableQuery(statement) ? 0 : -1;
			} else if (statement.toUpperCase().indexOf("INDEX") == 0 || statement.toUpperCase().indexOf("UNIQUE") == 0) {
				queryType = parseCreateIndexQuery(statement) ? 1 : -1;
			} else if (statement.toUpperCase().indexOf("PRIMARY") == 0) {
				queryType = parseCreatePrimaryKeyQuery(statement) ? 2 : -1;
			} else if (statement.toUpperCase().indexOf("DATABASE") == 0) {
				queryType = parseCreateDBQuery(statement) ? 3 : -1;
			}
		}
	}

	private boolean parseCreateTableQuery(String statement) {
		int tableIndex = statement.indexOf("table");

		statement = statement.substring(tableIndex + 5).trim();

		if (statement.length() == 0) {
			System.out.println("table name is missing");
			return false;
		}
		int lpIndex = statement.indexOf("(");
		int rpIndex = statement.lastIndexOf(")");
		if (lpIndex == -1 || rpIndex != statement.length() - 1) {
			System.out.println("parenthesis are missing");
			return false;
		}
		relationName = statement.substring(0, statement.indexOf("(")).trim();

		if (relationName.contains(" ")) {
			System.out.println("Rubbish between table name and opening parenthesis");
			return false;
		}

		statement = statement.substring(statement.indexOf(relationName) + relationName.length()).trim();

		StringTokenizer tokens = new StringTokenizer(statement.substring(statement.indexOf("(") + 1, statement.lastIndexOf(")")).trim(), ",");
		if (tokens.countTokens() == 0) {
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
			} else if (Attribute.stringToType(parsedToken.get(1)) == Attribute.Type.Char) {
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
				} else {
					System.out.println("Length needs to be specified for char data type as \"(length)\" !");
					return false;
				}
			} else {
				parsedToken.add("4");
			}
			if (attributeTokens.countTokens() < 3) {
				Boolean[] properties = new Boolean[2];
				Arrays.fill(properties, Boolean.FALSE);
				while (attributeTokens.hasMoreTokens()) {
					String token = attributeTokens.nextToken();
					if (token.equals("notnull") && !properties[0]) {
						properties[0] = true;
					} else if (token.equals("unique") && !properties[1]) {
						properties[1] = true;
					} else {
						System.out.println("Improper Syntax!");
						return false;
					}
				}
				parsedToken.add(properties[0].toString());
				parsedToken.add(properties[1].toString());
			} else {
				System.out.println("Only 4 properties per attribute i.e name , type , null/not_null , unique are allowed");
				return false;
			}
			parsedData.add(parsedToken);
		}
		return true;
	}

	private boolean parseCreatePrimaryKeyQuery(String statement) {
		int primarykeyIndex = statement.trim().indexOf("primary key ");
		int onIndex = statement.trim().indexOf("on");
		Vector<String> attributeList = new Vector<String>();
		if (primarykeyIndex != -1 && onIndex != -1) {
			if (primarykeyIndex <= onIndex) {
				statement = statement.substring(onIndex + 2).trim();
				if (statement.length() == 0) {
					System.out.println("table name is missing");
					return false;
				}
				int lp = statement.indexOf("(");
				if (lp != -1) {
					relationName = statement.substring(0, lp).trim();
				} else {
					relationName = statement.substring(0).trim();
				}

				long newRelationId = ObjectHolder.getObjectHolder().getRelationId(relationName);

				if (newRelationId != -1) {
					statement = statement.substring(statement.indexOf(relationName) + relationName.length()).trim();
					if (statement.length() == 0) {
						System.out.println("Parenthesis are missing");
						return false;
					}
					String primarykeyPart = "";
					primarykeyPart = statement.substring(statement.indexOf("(") + 1, statement.indexOf(")")).trim();

					if (primarykeyPart.length() != 0) {
						String[] primarykeyPartSplit = primarykeyPart.split(",");
						Relation newRelation = (Relation) ObjectHolder.getObjectHolder().getObject(newRelationId);
						Vector<Attribute> attributes = newRelation.getAttributes();

						for (int i = 0; i < primarykeyPartSplit.length; i++) {
							boolean chk = true;
							for (int k = 0; k < attributes.size(); k++) {
								if (attributes.get(k).getName().equals(primarykeyPartSplit[i])) {
									chk = false;
								}
							}
							if (chk) {
								QueryParser.print_error(1, primarykeyPartSplit[i]);
								return false;
							}
							attributeList.add(primarykeyPartSplit[i]);
						}
					} else {
						System.out.println("Primary key attribute is missing");
						return false;
					}
				} else {
					System.out.println(relationName + " is not a valid Relation Name");
					return false;
				}
			} else {
				System.out.println("Keyword \'on\' appears before keyword \'primary key\'");
				return false;
			}
		} else {
			System.out.println("Not a valid Primary Key syntax");
			return false;
		}
		parsedData.add(attributeList);
		return true;
	}

	private boolean parseCreateIndexQuery(String statement) {
		boolean unique = false;
		Vector<String> parsedToken;
		String attribtueName;
		if (statement.indexOf("unique ") == 0) {
			unique = true;
			statement = statement.substring(6).trim();
		}
		if (statement.indexOf("index ") == 0) {
			statement = statement.substring(5).trim();
		} else {
			System.out.println("Keyword index is missing");
			return false;
		}
		if (statement.length() == 0 || statement.startsWith("on ") || statement.indexOf(" ") <= 0) {
			System.out.println("Index name is missing");
			return false;
		}
		indexName = statement.substring(0, statement.indexOf(" ")).trim();
		statement = statement.substring(statement.indexOf(" ")).trim();
		if (statement.indexOf("on ") == 0) {
			statement = statement.substring(2).trim();
			if (statement.length() == 0) {
				System.out.println("table name is missing");
				return false;
			}
			if (statement.indexOf("(") != -1) {
				relationName = statement.substring(0, statement.indexOf("(")).trim();
				long newRelationId = ObjectHolder.getObjectHolder().getRelationId(relationName);
				if (newRelationId != -1) {
					Relation newRelation = (Relation) ObjectHolder.getObjectHolder().getObject(newRelationId);
					Map<String, Integer> relationAttributes = newRelation.getAttributesNames();
					parsedToken = new Vector<String>();
					statement = statement.substring(statement.indexOf("(")).trim();
					if (statement.startsWith("(") && statement.endsWith(")")) {
						statement = statement.substring(statement.indexOf("(") + 1, statement.lastIndexOf(")")).trim();
						StringTokenizer attribtueNames = new StringTokenizer(statement, ",");
						if (attribtueNames.countTokens() > 0) {
							while (attribtueNames.hasMoreTokens()) {
								attribtueName = attribtueNames.nextToken().trim();
								if (attribtueName.indexOf(" ") != -1) {
									System.out.println("All attributes must be seperated by ','. Error near : " + attribtueName);
									return false;
								}
								if (relationAttributes.containsKey(attribtueName)) {
									parsedToken.add(attribtueName);
								} else {
									QueryParser.print_error(1, attribtueName);
									return false;
								}
							}
							parsedData.add(parsedToken);
						} else {
							System.out.println("Atleat one attribute should be specified. Error near : " + statement);
							return false;
						}
					} else {
						System.out.println("Closing Parenthesis are missing at the end");
						return false;
					}
				} else {
					QueryParser.print_error(2, relationName);
					return false;
				}
			} else {
				System.out.println("Parenthesis are missing");
				return false;
			}
		} else {
			System.out.println("Keyword on is missing");
			return false;
		}
		parsedToken = new Vector<String>();
		parsedToken.add(String.valueOf(unique));
		parsedData.add(parsedToken);
		return true;
	}

	private boolean parseCreateDBQuery(String statement) {
		if (statement.length() >= 8) {
			statement = statement.substring(8).trim();
			if (statement.length() > 0 && !statement.contains(" ")) {
				dbName = statement;
				return true;
			}
			System.out.println("Error : database name not specified");
		}
		System.out.println("Error : Undefined Syntax!");
		return false;
	}

	public boolean executeOperation() {
		if (queryType == 0) {
			return DatabaseManager.getSystemCatalog().createTable(relationName, parsedData);
		} else if (queryType == 1) {
			return DatabaseManager.getSystemCatalog().createIndex(indexName, relationName, parsedData);
		} else if (queryType == 2) {
			return DatabaseManager.getSystemCatalog().addPrimaryKey(relationName, parsedData.get(0));
		} else if (queryType == 3) {
			return DatabaseManager.createDatabase(dbName);
		}
		return false;
	}
}
