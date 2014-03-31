package databaseManager;

import java.util.ArrayList;
import java.util.Date;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.StringTokenizer;

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
