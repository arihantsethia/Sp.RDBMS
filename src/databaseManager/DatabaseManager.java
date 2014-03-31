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
	
	public static void main(final String[] args){
		String input = "";
		DatabaseManager db = new DatabaseManager();
		Scanner scanner = new Scanner(System.in).useDelimiter(";");
		System.out.println("Welcome to sp.sql terminal.");
		while(true){
			System.out.print("sp.sql > ");
			input = scanner.next().replace("\n"," ").trim();
			System.out.println(input);
			if(input.equalsIgnoreCase("exit")){
				break;
			}else{
				//db.parseCommand(input);
			}
		}
		System.out.println("Bye!.");
	}
	
}
