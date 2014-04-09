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

    public void parseCommand(String query) {
	String[] splitCommand = query.split(" ");
	if (splitCommand[0].equals("create")) {
	    if (splitCommand[1].equals("table")) {
		if (systemCatalog.createTable(query) == true) {
		    System.out.println("Table " + splitCommand[2].trim() + " Successfully Created");
		} else {
		    System.out.println("Table " + splitCommand[2].trim() + " Already Exist");
		}
	    } else if (splitCommand[1].equals("index")) {

	    }
	} else if (splitCommand[0].equals("drop")) {
	    if (splitCommand[1].equals("table")) {
		if (systemCatalog.dropTable(splitCommand[2].trim()) == true) {
		    System.out.println("Table " + splitCommand[2].trim() + " Successfully Droped");
		} else {
		    System.out.println("Table " + splitCommand[2].trim() + " not Exist");
		}
	    } else if (splitCommand[1].equals("index")) {

	    }
	} else if (splitCommand[0].equals("select")) {

	} else if (splitCommand[0].equals("insert")) {
	    if (splitCommand[1].equals("into")) {
		if (systemCatalog.insertRecord(query) == true) {
		    System.out.println("Table " + splitCommand[2].trim() + " Successfully inserted");
		} else {
		    System.out.println("Table " + splitCommand[2].trim() + " can not insert");
		}
	    }
	}
    }
}
