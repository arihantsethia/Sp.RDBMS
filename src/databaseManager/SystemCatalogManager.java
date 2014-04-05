/**
 * 	@author Arihant , Arun and Vishavdeep
 *  Class SystemCatalogManager.java
 *  This class access methods of class BufferManager. and it's methods will be accessed by DatabaseManager class. 
 *   
 */


package databaseManager;

import java.util.ArrayList;
import java.util.Date;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.StringTokenizer;

/**
 *	@param
 *		ATTRIBUTE_CATALOG
 *			string containing value "attribute_catalog.cat"
 *		RELATION_CATALOG
 *			string containing value "relation_catalog.cat"
 *		bufferManager
 *			object of class named BufferManager. that object will be initiated when constructor of this class is called.
 *		attributeList
 *			list of attributes		
 */

public class SystemCatalogManager {

	public static final String ATTRIBUTE_CATALOG = "attribute_catalog.cat";
	public static final String RELATION_CATALOG = "relation_catalog.cat";

	private BufferManager bufferManager;
	private ArrayList<Attribute> attributesList;
	
	public SystemCatalogManager(){
		attributesList = new ArrayList<Attribute>();
		bufferManager = new BufferManager();
		loadRelationCatalog();
		loadAttributeCatalog();
	}
	
	public void loadRelationCatalog(){
		
	}
	
	public void loadAttributeCatalog(){
		
	}
}
