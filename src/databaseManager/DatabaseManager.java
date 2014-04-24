/**
 * 
 *	@author Arihant , Arun and Vishavdeep
 *	This Class is used to call other class named systemCatalog
 *
 */

package databaseManager;

import java.io.File;
import java.util.Vector;
import queriesManager.Operation;
import queriesManager.QueryParser;

public class DatabaseManager {

	private static SystemCatalogManager systemCatalog;
	private static Vector<String> dbHolder;
	public static String rootDir;
	public static boolean inDb;

	public DatabaseManager() {
		rootDir = System.getProperty("user.dir");
		dbHolder = new Vector<String>();
		inDb = false;
		loadDatabases();
	}

	public boolean loadDatabases() {
		File dir = new File(rootDir);
		String[] names = dir.list();
		for (String name : names) {
			if (new File(rootDir + "/" + name).isDirectory()) {
				if (name.startsWith("db_")) {
					dbHolder.add(name.substring(3));
				}
			}
		}
		dir = new File(".");
		if (dir.getAbsolutePath().equals(rootDir)) {
			return false;
		}
		return dbHolder.size() > 0 ? true : false;
	}

	public static boolean createDatabase(String dbName) {
		if (!dbHolder.contains(dbName)) {
			File dir = new File(rootDir + "/db_" + dbName);
			dir.mkdir();
			dbHolder.add(dbName);
			System.out.println("Database successfully created " + dbName);
			return true;
		}
		System.out.println("Error : Database already exists.");
		return false;
	}

	public static void showDatabases() {
		if (dbHolder.size() > 0) {
			for (int i = 0; i < dbHolder.size(); i++) {
				System.out.println((i + 1) + ". " + dbHolder.get(i));
			}
		} else {
			System.out.println("No database exists!");
		}
	}

	public static void useDatabase(String dbName) {
		if (dbHolder.contains(dbName)) {
			if (systemCatalog != null) {
				systemCatalog.close();
			}
			System.setProperty("user.dir", rootDir);
			inDb = false;
			File directory = new File(rootDir + "/db_" + dbName);
			boolean result = (System.setProperty("user.dir", directory.getAbsolutePath()) != null);
			systemCatalog = new SystemCatalogManager();
			if (result) {
				inDb = true;
				System.out.println("Current Database Change to : " + dbName);
			} else {
				System.out.println("Error : Couldn't change to database " + dbName + ".");
			}
		} else {
			System.out.println("Error : Database " + dbName + " doesn't exist.");
		}
	}

	public static boolean dropDatabase(String dbName) {
		if (dbHolder.contains(dbName)) {
			String currPath = System.getProperty("user.dir");
			if (currPath.equals(rootDir + "/db_" + dbName)) {
				systemCatalog.close();
				systemCatalog = null;
				inDb = false;
				System.setProperty("user.dir", rootDir);
			}
			File dir = new File(rootDir + "/db_" + dbName);
			deleteDirectory(dir);
			dbHolder.remove(dbName);
			System.out.println("Database successfully droped.");
			return true;
		}
		System.out.println("Error : Database doesn't exist.");
		return false;
	}

	public static SystemCatalogManager getSystemCatalog() {
		return systemCatalog;
	}

	public void close() {
		if (systemCatalog != null) {
			systemCatalog.close();
		}
	}

	/**
	 * parseCommand function splits the query statement into different parts using " " as
	 * delimiter. This function passes query statement to the makeOperation function and 
	 * calls the makeOperation function and executeOperation depending on the 0th part of
	 * query statement in String[] splitCommand. 
	 * @param query is the query statement executed by the user.
	 */
	public void parseCommand(String query) {
		if (query.contains(" database ") || query.contains(" databases ") || inDb) {
			query=query.trim();
			String[] splitCommand = query.split(" ");
			if (splitCommand[0].equals("create")) {
				Operation operation = Operation.makeOperation(query.trim());
				if(operation.executeOperation()){
					System.out.println("Operation Successfully Executed.");
				}
			} else if (splitCommand[0].equals("drop")) {
				Operation operation = Operation.makeOperation(query.trim());
				if(operation.executeOperation()){
					System.out.println("Operation Successfully Executed.");
				}
			} else if (query.contains("equi")) {
				Operation operation = Operation.makeOperation(query.trim());
				if(operation.executeOperation()){
					System.out.println("Operation Successfully Executed.");
				}
			} else if (query.contains("join")) {
				Operation operation = Operation.makeOperation(query.trim());
				if(operation.executeOperation()){
					System.out.println("Operation Successfully Executed.");
				}
			} else if (splitCommand[0].equals("select")) {
				if (QueryParser.isSelectStatementQuery(query)) {
					Operation operation = Operation.makeOperation(query.trim());
					if(operation.executeOperation()){
						System.out.println("Operation Successfully Executed.");
					}
				}
			} else if (splitCommand[0].equals("insert")) {
				if (QueryParser.isInsertStatementQuery(query)) {
					if (systemCatalog.insertRecord(query) == true) {
						System.out.println(" Successfully inserted into table : " + splitCommand[2].trim());
					} else {
						System.out.println("Can not insert table : " + splitCommand[2].trim());
					}
				}
			} else if (splitCommand[0].equals("show")) {
				if (splitCommand.length == 2) {
					if (splitCommand[1].equals("databases")) {
						showDatabases();
					} else if (splitCommand[1].equals("tables")) {
						System.out.println("List of Tables :- ");
						systemCatalog.showTables();
					} else {
						System.out.println("Error : Undefined Syntax.");
					}
				} else {
					System.out.println("Error : Undefined Syntax.");
				}
			} else if (splitCommand[0].equals("update")) {
				if (QueryParser.isUpdateStatementQuery(query)) {
					Operation operation = Operation.makeOperation(query.trim());
					if (operation.executeOperation()) {
						System.out.println("Successfully updated values ");
					}
				}
			} else if (splitCommand[0].equals("delete")) {
				if (QueryParser.isDeleteStatementQuery(query)) {
					Operation operation = Operation.makeOperation(query.trim());
					if (operation.executeOperation()) {
						System.out.println("Records successfully deleted ");
					}
				}
			} else if (splitCommand[0].equals("desc")) {
				if (systemCatalog.descOperation(query)) {
					System.out.println();
				}
			} else if (splitCommand[0].equals("use")) {
				if (splitCommand.length == 3) {
					if (splitCommand[1].equals("database")) {
						useDatabase(splitCommand[2]);
					} else {
						System.out.println("Error : Undefined Syntax.");
					}
				} else {
					System.out.println("Error : Undefined Syntax.");
				}
			} else {
				System.out.println("Error: undefined Syntax\n");
			}
		} else {
			System.out.println("Error: No Database Selected");
		}
	}

	private static boolean deleteDirectory(File directory) {
		if (directory.exists()) {
			File[] files = directory.listFiles();
			if (null != files) {
				for (int i = 0; i < files.length; i++) {
					if (files[i].isDirectory()) {
						deleteDirectory(files[i]);
					} else {
						files[i].delete();
					}
				}
			}
		}
		return (directory.delete());
	}
}
