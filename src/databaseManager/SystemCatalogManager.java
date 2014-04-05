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
 * @param ATTRIBUTE_CATALOG
 *            string containing value "attribute_catalog.cat" RELATION_CATALOG
 *            string containing value "relation_catalog.cat" bufferManager
 *            object of class named BufferManager. that object will be initiated
 *            when constructor of this class is called. attributeList list of
 *            attributes
 */

public class SystemCatalogManager {

	public static final String ATTRIBUTE_CATALOG = "attribute_catalog.cat";
	public static final String RELATION_CATALOG = "relation_catalog.cat";
	public static final int ATTRIBUTE_RECORD_SIZE = 500;
	public static final long ATTRIBUTE_CATALOG_ID = 1;
	public static final int RELATION_RECORD_SIZE = 500;
	public static final long RELATION_CATALOG_ID = 0;

	private BufferManager bufferManager;
	private ArrayList<Attribute> attributesList;
	private RelationHolder relationHolder;
	private long totalAttributesCount;
	private long totalRelationsCount;

	public SystemCatalogManager() {
		attributesList = new ArrayList<Attribute>();
		bufferManager = new BufferManager();
		relationHolder = RelationHolder.getRelationHolder();
		loadRelationCatalog();
		loadAttributeCatalog();
	}

	public void loadRelationCatalog() {
		totalRelationsCount = 0;
	}

	public void loadAttributeCatalog() {
		totalAttributesCount = 0;
	}

	public void updateRelationCatalog(long relationId, ByteBuffer bufferStream) {

	}

	public boolean createTable(String relationStmt) {
		relationStmt = relationStmt.substring(
				relationStmt.toLowerCase().indexOf("table") + 5).trim();
		String relationName = relationStmt.substring(0,
				relationStmt.indexOf("(")).trim();
		Relation newRelation = new Relation(relationName, totalRelationsCount);
		if (relationHolder.addRelation(newRelation)) {
			addRelationToCatalog(newRelation);
			StringTokenizer tokens = new StringTokenizer(
					relationStmt.substring(relationStmt.indexOf("(") + 1,
							relationStmt.indexOf(")")), ",");
			while (tokens.hasMoreTokens()) {
				StringTokenizer attributeTokens = new StringTokenizer(tokens
						.nextToken().trim(), " ");
				if (attributeTokens.countTokens() < 2) {
					System.out
							.println("Name and type of attribute needs to be specified");
					return false;
				}
				String attributeName = attributeTokens.nextToken().trim();
				String _attributeType = attributeTokens.nextToken().trim();
				Attribute.Type attributeType = Attribute
						.stringToType(_attributeType);
				boolean nullable = true;
				int size = Attribute.Type.getSize(attributeType);
				if (attributeType == Attribute.Type.Char) {
					size = size
							* Integer.parseInt(_attributeType.substring(
									_attributeType.indexOf("(") + 1,
									_attributeType.indexOf(")")).trim());
				}
				if (attributeTokens.countTokens() > 1) {
					System.out
							.println("Only 3 properties per attribute i.e name , type , null/not_null are allowed");
					return false;
				} else if (attributeTokens.countTokens() == 1) {
					if (attributeTokens.nextToken().trim()
							.equalsIgnoreCase("not_null")) {
						nullable = false;
					} else {
						System.out
								.println("Only 3 properties per attribute i.e name , type , null/not_null are allowed");
						return false;
					}
				}
				Attribute newAttribute = new Attribute(attributeName,
						attributeType, totalAttributesCount,
						newRelation.getRelationId(), size, nullable);
				addAttributeToCatalog(newAttribute);
			}
			updateRelationCatalog(newRelation.getRelationId(),
					newRelation.serialize());
			System.out.println("Created new table " + relationName + " :-)");
		} else {
			System.out.println("Table " + relationName + " already exists!");
		}
		return true;
	}

	private void addAttributeToCatalog(Attribute newAttribute) {
		// TODO Auto-generated method stub
		int attributeRecordsPerBlock = (int) (DiskSpaceManager.BLOCK_SIZE * 8
				/ (1 + 8 * ATTRIBUTE_RECORD_SIZE));
		long freeBlockNumber = bufferManager
				.getFreeBlockNumber(ATTRIBUTE_CATALOG_ID);
		int freeRecordOffset = bufferManager.getFreeRecordOffset(
				RELATION_CATALOG_ID, freeBlockNumber, attributeRecordsPerBlock,
				ATTRIBUTE_RECORD_SIZE);
		bufferManager.write(1, freeBlockNumber, freeRecordOffset,
				newAttribute.serialize());
		totalAttributesCount++;
	}

	private void addRelationToCatalog(Relation newRelation) {
		// TODO Auto-generated method stub
		int relationRecordsPerBlock = (int) (DiskSpaceManager.BLOCK_SIZE * 8
				/ (1 + 8 * RELATION_RECORD_SIZE));
		long freeBlockNumber = bufferManager
				.getFreeBlockNumber(RELATION_CATALOG_ID);
		int freeRecordOffset = bufferManager.getFreeRecordOffset(
				RELATION_CATALOG_ID, freeBlockNumber, relationRecordsPerBlock,
				RELATION_RECORD_SIZE);
		bufferManager.write(0, freeBlockNumber, freeRecordOffset,
				newRelation.serialize());
	}

	public void close() {
		// TODO Auto-generated method stub
		bufferManager.flush();
	}
}
