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
import queriesManager.SelectOperation;

public class DatabaseManager {

	private static SystemCatalogManager systemCatalog;
	private Vector<String> dbHolder;
	public static String rootDir;
	public static boolean inDb;

	public DatabaseManager() {
		File directory = new File(".");
		rootDir = directory.getAbsolutePath();
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

	public boolean createDatabase(String dbName) {
		if (!dbHolder.contains(dbName)) {
			File dir = new File(rootDir + "/db_" + dbName);
			dir.mkdir();
			dbHolder.add(dbName);
			return true;
		}
		return false;
	}

	public void showDatabases() {
		if (dbHolder.size() > 0) {
			for (int i = 0; i < dbHolder.size(); i++) {
				System.out.println((i + 1) + ". " + dbHolder.get(i));
			}
		} else {
			System.out.println("No database exists!");
		}
	}

	public static void useDatabase(String dbName) {
		if (systemCatalog != null) {
			systemCatalog.close();
		}
		System.setProperty("user.dir", rootDir);
		File directory = new File("db_" + dbName).getAbsoluteFile();
		boolean result = (System.setProperty("user.dir", directory.getAbsolutePath()) != null);
		systemCatalog = new SystemCatalogManager();
		if (result) {
			inDb = true;
			System.out.println("Current Database Change to : " + dbName);
		} else {
			System.out.println("Error : Database " + dbName + " doesn't exist.");
		}
	}

	public boolean dropDatabse(String dbName) {
		if (dbHolder.contains(dbName)) {
			File dir = new File(".");
			String currPath = dir.getAbsolutePath();
			System.setProperty("user.dir", rootDir);
			dir = new File(rootDir + "/db_" + dbName);
			deleteDirectory(dir);
			dbHolder.remove(dbName);
			dir = new File(currPath);
			if (dir.exists() && dir.isDirectory()) {
				System.setProperty("user.dir", currPath);
			} else {
				inDb=false;
				systemCatalog.close();
				systemCatalog = null;
			}
			return true;
		}
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

	public void parseCommand(String query) {
		String[] splitCommand = query.split(" ");
		if (splitCommand[0].equals("create")) {
			Operation operation = Operation.makeOperation(query.trim());
			operation.executeOperation();
		} else if (splitCommand[0].equals("drop")) {
			Operation operation = Operation.makeOperation(query.trim());
			operation.executeOperation();
		} else if (splitCommand[0].equals("select")) {
			if (QueryParser.isSelectStatementQuery(query)) {
				Operation operation = Operation.makeOperation(query.trim());
				operation.executeOperation();
			}
		} else if (splitCommand[0].equals("insert")) {
			if (QueryParser.isInsertStatementQuery(query)) {
				if (splitCommand[1].equals("into")) {
					if (systemCatalog.insertRecord(query) == true) {
						System.out.println(" Successfully inserted into Table :" + splitCommand[2].trim());
					} else {
						System.out.println("Can not insert Table :" + splitCommand[2].trim());
					}
				}
			}
		} else if (splitCommand[0].equals("show")) {
			if (query.replace(" ", "").trim().equals("showtables") && query.split(" ").length == 2) {
				System.out.println("List of Tables :- ");
				systemCatalog.showTables();
			} else {
				System.out.println("wrong show table syntax");
			}
		} else if (splitCommand[0].equals("update")) {
			if (QueryParser.isUpdateStatementQuery(query)) {
				Operation operation = Operation.makeOperation(query.trim());
				if (operation.executeOperation()) {
					System.out.println(" Successfully updated values ");
				}

			}

		} else if (splitCommand[0].equals("delete")) {
			if (QueryParser.isDeleteStatementQuery(query)) {
				Operation operation = Operation.makeOperation(query.trim());
				if (operation.executeOperation()) {
					System.out.println(" Data successfully deleted ");
				}
			}
		} else if (splitCommand[0].equals("desc")) {
			if (systemCatalog.descOperation(query)) {
				System.out.println();
			}
		} else {
			System.out.println("undefined Syntax\n");
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
