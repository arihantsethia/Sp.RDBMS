package queriesManager;

import java.util.StringTokenizer;
import java.util.Vector;

import databaseManager.DatabaseManager;

public class DropOperation extends Operation {

	protected String indexName;
	protected String relationName;
	protected int queryType;

	DropOperation(String statement) {
		setType(QueryParser.OperationType.DROP);
		statement = statement.substring(statement.toUpperCase().indexOf("DROP") + 4).trim();
		if (statement.toUpperCase().indexOf("TABLE") == 0) {
			queryType = parseDropTableQuery(statement) ? 0 : -1;
		} else if (statement.toUpperCase().indexOf("INDEX") == 0) {
			queryType = parseDropIndexQuery(statement) ? 1 : -1;
		}
	}

	private boolean parseDropIndexQuery(String statement) {
		statement = statement.substring(statement.toUpperCase().indexOf("TABLE") + 5).trim();
		relationName = statement.substring(0, statement.indexOf(" ")).trim();
		if (relationName.contentEquals(statement)) {
			return true;
		} else {
			return false;
		}
	}

	private boolean parseDropTableQuery(String statement) {
		statement = statement.substring(statement.toUpperCase().indexOf("INDEX") + 5).trim();
		StringTokenizer tokens = new StringTokenizer(statement, " ");
		if (tokens.countTokens() == 4) {
			indexName = tokens.nextToken().trim();
			if(tokens.nextToken().trim().equalsIgnoreCase("ON")){
				if(tokens.nextToken().trim().equalsIgnoreCase("TABLE")){
					relationName = tokens.nextToken().trim();
					return true;
				}else{
					return false;
				}
			}else{
				return false;
			}
		} else {
			return false;
		}
	}
	
	public boolean executeOperation() {
		if (queryType == 0) {
			return DatabaseManager.getSystemCatalog().dropTable(relationName);
		} else if (queryType == 1) {
			return DatabaseManager.getSystemCatalog().dropIndex(indexName, relationName);
		}
		return false;
	}
}
