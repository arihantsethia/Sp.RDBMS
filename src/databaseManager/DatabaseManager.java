/**
 * 
 *	@author Arihant , Arun and Vishavdeep
 *	This Class is used to call other class named systemCatalog
 *
 */

package databaseManager;

import queriesManager.Operation;
import queriesManager.SelectOperation;

public class DatabaseManager {

	private static SystemCatalogManager systemCatalog;

	public DatabaseManager() {
		systemCatalog = new SystemCatalogManager();
	}

	public static SystemCatalogManager getSystemCatalog() {
		return systemCatalog;
	}

	public void close() {
		systemCatalog.close();
	}

	public void parseCommand(String query) {
		String[] splitCommand = query.split(" ");
		if (splitCommand[0].equals("create")) {
			Operation operation = Operation.makeOperation(query.trim());
			operation.executeOperation();
		} else if (splitCommand[0].equals("drop")) {
			Operation operation = Operation.makeOperation(query.trim());
			operation.executeOperation();
		} else if (splitCommand[0].equals("select")) {
			Operation operation = Operation.makeOperation(query.trim());
			operation.executeOperation();
		} else if (splitCommand[0].equals("insert")) {
			if (splitCommand[1].equals("into")) {
				if (systemCatalog.insertRecord(query) == true) {
					System.out.println(" Successfully inserted into Table :" + splitCommand[2].trim());
				} else {
					System.out.println("Can not insert Table :" + splitCommand[2].trim());
				}
			}
		} else if (splitCommand[0].equals("show")) {
			if (splitCommand[1].equals("tables")) {
				System.out.println("List of Tables.");
				systemCatalog.showTables();
			}
		}else if(splitCommand[0].equals("update")){
			Operation operation = Operation.makeOperation(query.trim());
			operation.executeOperation();
		}else if(splitCommand[0].equals("delete")){
			Operation operation = Operation.makeOperation(query.trim());
			operation.executeOperation();
		}else if(splitCommand[0].equals("desc")){
			System.out.println("") ;
			if (systemCatalog.descOperation(query)) {
					System.out.println();
			} else {
					System.out.println("wrong statement" + splitCommand[2].trim());
			}
		}
	}
}
