package databaseManager;
import java.util.ArrayList;

public class Utility {
	private static Utility uty ;
	
	public static Utility getUtility(){
		if(uty!=null){
			return uty ;
		}
		uty  = new Utility() ;
		return uty ;
	}
	
	public boolean isSameType(String type1 , String Type2){
		if(type1.equals("int")){
			try { 
		        Integer.parseInt(Type2); 
		        return true ;
			} catch(NumberFormatException e) { 
		        return false; 
		    }
		}else if(type1.equals("char")){
			if(Type2.length()==3 && Type2.charAt(0)=='\'' && Type2.charAt(2)=='\''){
				return true ;
			}
		}else if(type1.equals("float")){
			
		}
		return false ;
	}
	
	public int stringToInt(String s){
		return Integer.parseInt(s) ;
	}
	
	public char stringToChar(String s){
			return s.charAt(1) ;
	}
	
	public boolean isVarChar(String s , int size)
	{
		s = s.trim();
		if(s.length() >= 2 && s.length() <= size )
		{
			if(s.charAt(0)=='\'' && s.charAt(s.length()-1)=='\''){
				return true ;
			}
			return false ;
		}
		return false ;
	}
	
}
