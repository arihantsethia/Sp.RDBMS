package queriesManager;

import java.util.StringTokenizer;

import databaseManager.DatabaseManager;

public class DropOperation extends Operation {

	protected String indexName;
	protected String relationName;
	protected String dbName;
	protected int queryType;

	DropOperation(String statement) {
		queryType = -1;
		setType(QueryParser.OperationType.DROP);
		int dropIndex = statement.trim().indexOf("drop");
		if (dropIndex == 0) {
			statement = statement.substring(statement.indexOf("drop") + 4).trim();
			if (statement.indexOf("table") == 0) {
				queryType = parseDropTableQuery(statement) ? 0 : -1;
			} else if (statement.indexOf("index") == 0) {
				queryType = parseDropIndexQuery(statement) ? 1 : -1;
			} else if (statement.indexOf("primary key") == 0) {
				queryType = parseDropPrimaryKeyQuery(statement) ? 2 : -1;
			} else if (statement.indexOf("database") == 0) {
				queryType = parseDropDBQuery(statement) ? 3 : -1;
			}
		}
	}

	private boolean parseDropPrimaryKeyQuery(String statement) {
		return true;
	}

	private boolean parseDropTableQuery(String statement) {
		statement = statement.substring(statement.indexOf("table") + 5).trim();
		if (statement.length() == 0) {
			System.out.println("Table name is missing");
			return false;
		}
		relationName = statement.substring(0, statement.indexOf(" ")).trim();
		if (relationName.contentEquals(statement)) {
			return true;
		} else {
			return false;
		}
	}

	private boolean parseDropIndexQuery(String statement) {
		statement = statement.substring(statement.indexOf("index") + 5).trim();
		StringTokenizer tokens = new StringTokenizer(statement, " ");
		if (tokens.countTokens() == 4) {
			indexName = tokens.nextToken().trim();
			if (tokens.nextToken().trim().equalsIgnoreCase("ON")) {
				if (tokens.nextToken().trim().equalsIgnoreCase("TABLE")) {
					relationName = tokens.nextToken().trim();
					return true;
				} else {
					return false;
				}
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	private boolean parseDropDBQuery(String statement) {
		if (statement.length() >= 8) {
			statement = statement.substring(8).trim();
			if (statement.length() > 0 && !statement.contains(" ")) {
				dbName = statement;
				return true;
			}
			System.out.println("Error : database name not specified");
			return false;
		}
		System.out.println("Error : Undefined Syntax!");
		return false;
	}

	public boolean executeOperation() {
		if (queryType == 0) {
			return DatabaseManager.getSystemCatalog().dropTable(relationName);
		} else if (queryType == 1) {
			return DatabaseManager.getSystemCatalog().dropIndex(indexName, relationName);
		} else if (queryType == 2) {
			return DatabaseManager.getSystemCatalog().dropIndex(relationName + "_pk", relationName);
		} else if (queryType == 3) {
			return DatabaseManager.dropDatabase(dbName);
		}
		return false;
	}
}