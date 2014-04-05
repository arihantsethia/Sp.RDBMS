package databaseManager;

import java.util.Scanner;

/**
 * 
 *	@author bhati
 *	This Class is used to call other class named systemCatalog
 *
 */

public class DatabaseManager {
	
	private static SystemCatalogManager systemCatalog;
	
	public DatabaseManager(){
		systemCatalog = new SystemCatalogManager();
	}
	
	public static SystemCatalogManager getSystemCatalog(){
		return systemCatalog; 
	}
}
