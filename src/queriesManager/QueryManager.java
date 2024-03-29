package queriesManager;

import java.util.Scanner;
import databaseManager.DatabaseManager;

public class QueryManager {
	private static String input;
	private static DatabaseManager databaseManager;
	private static Scanner inputScanner;

	public static void main(String[] args) {
		input = "";
		databaseManager = new DatabaseManager();
		inputScanner = new Scanner(System.in).useDelimiter(";");
		System.out.println(" \t\t\t** Welcome to Sp.Sql ** ");
		while (!input.toLowerCase().trim().equals("exit")) {
			System.out.print("Sp.Sql-> ");
			input = inputScanner.next().trim();
			if (!input.equals("exit")) {
				if(input.equals("commit")){
					databaseManager.commit();
					System.out.println("Database Successfully Committed.");
				}else{
					databaseManager.parseCommand(input+" ");
				}
			} else {
				System.out.println("Byee!");
				databaseManager.close();
				break;
			}
			System.out.println();
		}
	}
}
