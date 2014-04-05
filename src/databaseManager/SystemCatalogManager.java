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
import java.nio.ByteBuffer;
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
	public static final long ATTRIBUTE_RECORD_SIZE = 500;
	public static final long RELATION_RECORD_SIZE = 500;
	

	private BufferManager bufferManager;
	private ArrayList<Attribute> attributesList;
	private RelationHolder relationHolder;
	
	public SystemCatalogManager(){
		attributesList = new ArrayList<Attribute>();
		bufferManager = new BufferManager();
		relationHolder = RelationHolder.getRelationHolder();
		loadRelationCatalog();
		loadAttributeCatalog();
	}
	
	public void loadRelationCatalog(){
		
	}
	
	public void loadAttributeCatalog(){
		
	}
	
	public void updateRelationCatalog(long relationId,ByteBuffer bufferStream){
		
	}
	
	public boolean createTable(String relationStmt){
		String relationName = relationStmt;
		Relation newRelation = new Relation(relationName, relationHolder.getNewId());
		if(relationHolder.addRelation(newRelation)){
			/* Add to relation catalog */
			updateRelationCatalog(newRelation.getRelationId(), newRelation.getMetaData());
			System.out.println("Created new table "+relationName+" :-)");
		}else{
			System.out.println("Table "+relationName+" already exists!");
		}
		return true;
	}
}
