package queriesManager;

import java.util.StringTokenizer;

import databaseManager.DatabaseManager;
import databaseManager.ObjectHolder;

/**
 * 
 * The instance of "DropOperation" class is called whenever we want to execute
 * Create query. 
 */
public class DropOperation extends Operation {

	protected String indexName;
	protected String relationName;
	protected String dbName;
	protected int queryType;

	/**
	 * 
	 * The instance of "DropOperation" class is called whenever we want to execute
	 * Drop query.
	 * 
	 */
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

	/**
	 * It parse drop primary key query and give result true or false according to that. 
	 * @param statement
	 * @return
	 */
	private boolean parseDropPrimaryKeyQuery(String statement) {
		statement = statement.substring(statement.indexOf("primary key") + 11).trim();
		if(statement.length() == 0){
			System.out.println("Keyword \'on\' is missing");
			return false;
		}
		int onIndex = statement.indexOf("on");
		if(onIndex == 0){
			statement = statement.substring(onIndex + 2).trim();
			if(statement.length() == 0){
				System.out.println("table name is missing");
				return false;
			}
			relationName = statement.substring(0);
			long newRelationId = ObjectHolder.getObjectHolder().getRelationId(relationName);
			if(newRelationId != -1){
				System.out.println("primary key dropped successfully!");
				return true;
			}
			else{
				System.out.println("Not a valid relation name");
				return false;
			}
		}
		else{
			System.out.println("Not a valid drop primary key syntax");
		}
		
		return true;
	}

	/**
	 * It parse the drop statement query given as input and return true or false according to that.
	 * @param statement
	 * @return
	 */
	private boolean parseDropTableQuery(String statement) {
		statement = statement.substring(statement.indexOf("table") + 5).trim();
		if (statement.length() == 0) {
			System.out.println("Table name is missing");
			return false;
		}
		relationName = statement.substring(0).trim();
		long newRelationId = ObjectHolder.getObjectHolder().getRelationId(relationName);
		if(newRelationId != -1){
			//System.out.println("relation name is valid");
			System.out.println("table dropped successfully!");
			return true;
		}
		else{
			System.out.println("Not a valid relation name");
			return false;
		}
		//relationName = statement.substring(0, statement.indexOf(" ")).trim();
		/*if (relationName.contentEquals(statement)) {
			System.out.println("Relation name matched");
			return true;
		} else {
			System.out.println("Not a valid Relation Name");
			return false;
		}*/
	}

	/**
	 * It parse drop index key query and give result true or false according to that. 
	 * @param statement
	 * @return
	 */
	private boolean parseDropIndexQuery(String statement) {
		statement = statement.substring(statement.indexOf("index") + 5).trim();
		if(statement.length() == 0){
			System.out.println("index name is missing");
			return false;
		}
		
		if(statement.contains(" ")){
			indexName = statement.substring(0,statement.indexOf(" ")).trim();
			statement = statement.substring(statement.indexOf(indexName) + indexName.length()).trim();
			if(statement.length() == 0){
				System.out.println("Keyword \'on\' is missing");
				return false;
			}
			int onIndex = statement.indexOf("on ");
			if(onIndex == 0){
				statement = statement.substring(onIndex + 2).trim();
				if(statement.length() == 0){
					System.out.println("table name is missing");
					return false;
				}
				relationName = statement.substring(0);
				long newRelationId = ObjectHolder.getObjectHolder().getRelationId(relationName);
				if(newRelationId != -1){
					System.out.println("index dropped successfully!");
					return true;
				}
				else{
					System.out.println("Not a valid relation name");
					return false;
				}
			}
			else{
				System.out.println("Not a valid drop index syntax");
				return false;
			}
		}
		else{
			System.out.println("Not a valid drop index syntax");
			return false;
		}
	}

	/**
	 * parser for delete database query.
	 * @param statement
	 * @return
	 */
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

	/**
	 * This method will execute delete query and will reply true if query
	 * successfully executed. It evaluates according to query type whether it is delete index or primary key.
	 */
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