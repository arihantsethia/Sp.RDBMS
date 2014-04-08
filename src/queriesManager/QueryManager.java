package queriesManager;

import java.util.Scanner;
import databaseManager.DatabaseManager ;
public class QueryManager {
	private static String input ;
	private static DatabaseManager databaseManager ;
	private static Scanner inputScanner  ; 
	
	public static void main(String[] args){
		input = "" ;
		databaseManager = new DatabaseManager() ;
		inputScanner    = new Scanner(System.in).useDelimiter(";");
		
		while(!input.toLowerCase().trim().equals("exit")){
			input = inputScanner.next().toLowerCase().trim() ;
			if (!input.equals("exit")) {
    			databaseManager.parseCommand(input) ;
    		} else {
    			databaseManager.close() ;
    			System.exit(1);
    		}
		}
	}
}
