/**
 * 
 *	@author Arihant , Arun and Vishavdeep
 *	This Class is used to call other class named systemCatalog
 *
 */

package databaseManager;

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
	
	public void parseCommand(String stmt){
		systemCatalog.createTable(stmt);
	}
}
