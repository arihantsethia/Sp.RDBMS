/**
 * 
 *	@author Arihant , Arun and Vishavdeep
 *	This Class is used to call other class named systemCatalog
 *
 */

package databaseManager;

import java.util.Scanner;

public class DatabaseManager {
	
	private static SystemCatalogManager systemCatalog;
	
	public DatabaseManager(){
		systemCatalog = new SystemCatalogManager();
	}
	
	public static SystemCatalogManager getSystemCatalog(){
		return systemCatalog; 
	}
}