/**
 * @author Arihant , Arun and Vishavdeep
 * Class SystemCatalogManager.java
 * This class access methods of class BufferManager. and it's methods will be accessed by DatabaseManager class. 
 *   
 */

package databaseManager;

import java.util.Vector;
import java.nio.ByteBuffer;
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

    public static final String RELATION_CATALOG = "relation_catalog.db";
    public static final String ATTRIBUTE_CATALOG = "attribute_catalog.db";
    public static final String INDEX_CATALOG = "index_catalog.db";
    public static final int RELATION_RECORD_SIZE = 160;
    public static final long RELATION_CATALOG_ID = 0;
    public static final int ATTRIBUTE_RECORD_SIZE = 130;
    public static final long ATTRIBUTE_CATALOG_ID = 1;
    public static final int INDEX_RECORD_SIZE = 144;
    public static final long INDEX_CATALOG_ID = 2;

    private BufferManager bufferManager;
    private ObjectHolder objectHolder;
    public long totalAttributesCount;
    public long totalObjectsCount;

    public SystemCatalogManager() {
	bufferManager = BufferManager.getBufferManager();
	objectHolder = ObjectHolder.getObjectHolder();
	totalObjectsCount = 3;
	loadRelationCatalog();
	loadAttributeCatalog();
	loadIndexCatalog();
    }

    public void loadRelationCatalog() {
	Relation tableRelation = new Relation("relation_catalog", RELATION_CATALOG_ID);
	tableRelation.setRecordSize(RELATION_RECORD_SIZE);
	objectHolder.addObject(tableRelation);
	Relation tempRelation;
	Iterator relationIterator = new Iterator(tableRelation);
	while (relationIterator.hasNext()) {
	    ByteBuffer relationRecord = relationIterator.getNext();
	    if (relationRecord != null) {
		relationRecord.position(0);
		tempRelation = new Relation(relationRecord);
		if (totalObjectsCount <= tempRelation.getRelationId()) {
		    totalObjectsCount = tempRelation.getRelationId() + 1;
		}
		objectHolder.addObject(tempRelation);
	    }
	}
    }

    public void loadAttributeCatalog() {
	Relation attributeRelation = new Relation("attribute_catalog", ATTRIBUTE_CATALOG_ID);
	attributeRelation.setRecordSize(ATTRIBUTE_RECORD_SIZE);
	objectHolder.addObject(attributeRelation);
	Attribute tempAttribute;
	Iterator attributeIterator = new Iterator(attributeRelation);
	while (attributeIterator.hasNext()) {
	    ByteBuffer attributeRecord = attributeIterator.getNext();
	    if (attributeRecord != null) {
		attributeRecord.position(0);
		tempAttribute = new Attribute(attributeRecord);
		if (totalAttributesCount <= tempAttribute.getId()) {
		    totalAttributesCount = tempAttribute.getId() + 1;
		}
		objectHolder.addObjectToRelation(tempAttribute, false);
	    }
	}
    }

    public void loadIndexCatalog() {
	Relation indexRelation = new Relation("index_catalog", INDEX_CATALOG_ID);
	indexRelation.setRecordSize(INDEX_RECORD_SIZE);
	objectHolder.addObject(indexRelation);
	Index tempIndex;
	Iterator indexIterator = new Iterator(indexRelation);
	while (indexIterator.hasNext()) {
	    ByteBuffer indexRecord = indexIterator.getNext();
	    if (indexRecord != null) {
		indexRecord.position(0);
		tempIndex = new Index(indexRecord);
		if (totalObjectsCount < tempIndex.getIndexId()) {
		    totalObjectsCount = tempIndex.getIndexId() + 1;
		}
		objectHolder.addObject(tempIndex);
	    }
	}
    }

    public void updateRelationCatalog(long relationId, ByteBuffer bufferStream) {

    }

    public boolean createTable(String relationStmt) {
	relationStmt = relationStmt.substring(relationStmt.toLowerCase().indexOf("table") + 5).trim();
	String relationName = relationStmt.substring(0, relationStmt.indexOf("(")).trim();
	Relation newRelation = new Relation(relationName, totalObjectsCount);
	if (objectHolder.getRelationIdByRelationName(relationName) == -1) {
	    StringTokenizer tokens = new StringTokenizer(relationStmt.substring(relationStmt.indexOf("(") + 1, relationStmt.lastIndexOf(")")), ",");
	    while (tokens.hasMoreTokens()) {
		StringTokenizer attributeTokens = new StringTokenizer(tokens.nextToken().trim(), " ");
		if (attributeTokens.countTokens() < 2) {
		    System.out.println("Name and type of attribute needs to be specified");
		    return false;
		}
		String attributeName = attributeTokens.nextToken().trim();
		String _attributeType = attributeTokens.nextToken().trim();
		Attribute.Type attributeType = Attribute.stringToType(_attributeType);
		boolean nullable = true;
		int size = Attribute.Type.getSize(attributeType);
		if (attributeType == Attribute.Type.Char) {
		    size = size * Integer.parseInt(_attributeType.substring(_attributeType.indexOf("(") + 1, _attributeType.indexOf(")")).trim());
		}
		if (attributeTokens.countTokens() > 1) {
		    System.out.println("Only 3 properties per attribute i.e name , type , null/not_null are allowed");
		    return false;
		} else if (attributeTokens.countTokens() == 1) {
		    if (attributeTokens.nextToken().trim().equalsIgnoreCase("not_null")) {
			nullable = false;
		    } else {
			System.out.println("Only 3 properties per attribute i.e name , type , null/not_null are allowed");
			return false;
		    }
		}
		Attribute newAttribute = new Attribute(attributeName, attributeType, totalAttributesCount, newRelation.getRelationId(), size, nullable);
		newRelation.addAttribute(newAttribute, true);
		addAttributeToCatalog(newAttribute);
	    }
	    objectHolder.addObject(newRelation);
	    addRelationToCatalog(newRelation);
	    // updateRelationCatalog(newRelation.getRelationId(),
	    // newRelation.serialize());
	    System.out.println("Created new table " + relationName + " :-)");
	} else {
	    System.out.println("Table " + relationName + " already exists!");
	}
	return true;
    }

    public void addAttributeToCatalog(Attribute newAttribute) {
	int attributeRecordsPerPage = (int) (DiskSpaceManager.PAGE_SIZE * 8 / (1 + 8 * ATTRIBUTE_RECORD_SIZE));
	long freePageNumber = bufferManager.getFreePageNumber(ATTRIBUTE_CATALOG_ID);
	int freeRecordOffset = bufferManager.getFreeRecordOffset(ATTRIBUTE_CATALOG_ID, freePageNumber, attributeRecordsPerPage, ATTRIBUTE_RECORD_SIZE);
	bufferManager.write(ATTRIBUTE_CATALOG_ID, freePageNumber, freeRecordOffset, newAttribute.serialize());
	int recordNumber = (freeRecordOffset - (attributeRecordsPerPage + 7) / 8) / ATTRIBUTE_RECORD_SIZE;
	bufferManager.writeRecordBitmap(ATTRIBUTE_CATALOG_ID, freePageNumber, attributeRecordsPerPage, recordNumber, true);
	totalAttributesCount++;
    }

    public void addRelationToCatalog(Relation newRelation) {
	int relationRecordsPerPage = (int) (DiskSpaceManager.PAGE_SIZE * 8 / (1 + 8 * RELATION_RECORD_SIZE));
	long freePageNumber = bufferManager.getFreePageNumber(RELATION_CATALOG_ID);
	int freeRecordOffset = bufferManager.getFreeRecordOffset(RELATION_CATALOG_ID, freePageNumber, relationRecordsPerPage, RELATION_RECORD_SIZE);
	int recordNumber = (freeRecordOffset - (relationRecordsPerPage + 7) / 8) / RELATION_RECORD_SIZE;
	newRelation.setAddress(freePageNumber, recordNumber);
	bufferManager.write(RELATION_CATALOG_ID, freePageNumber, freeRecordOffset, newRelation.serialize());
	bufferManager.writeRecordBitmap(RELATION_CATALOG_ID, freePageNumber, relationRecordsPerPage, recordNumber, true);
	totalObjectsCount++;
    }

    public void addIndexToCatalog(Index newIndex) {
	int indexRecordsPerPage = (int) (DiskSpaceManager.PAGE_SIZE * 8 / (1 + 8 * INDEX_RECORD_SIZE));
	long freePageNumber = bufferManager.getFreePageNumber(RELATION_CATALOG_ID);
	int freeRecordOffset = bufferManager.getFreeRecordOffset(RELATION_CATALOG_ID, freePageNumber, indexRecordsPerPage, RELATION_RECORD_SIZE);
	int recordNumber = (freeRecordOffset - (indexRecordsPerPage + 7) / 8) / RELATION_RECORD_SIZE;
	newIndex.setAddress(freePageNumber, recordNumber);
	bufferManager.write(RELATION_CATALOG_ID, freePageNumber, freeRecordOffset, newIndex.serialize());
	bufferManager.writeRecordBitmap(RELATION_CATALOG_ID, freePageNumber, indexRecordsPerPage, recordNumber, true);
	totalObjectsCount++;
    }

    public void close() {
	bufferManager.flush();
    }

    public boolean dropTable(String relationName) {
	long newRelationId = objectHolder.getRelationIdByRelationName(relationName);
	if (newRelationId != -1) {
	    Relation newRelation = (Relation) objectHolder.getObject(newRelationId);
	    bufferManager.writeRecordBitmap(newRelationId, newRelation.getPageNumber(), newRelation.getRecordsPerPage(), newRelation.getRecordNumber(), false);
	    bufferManager.closeFile(newRelationId);
	    bufferManager.deleteFile(newRelation.getFileName());
	    objectHolder.removeObject(newRelationId);
	    return true;
	}
	return false;
    }

    public boolean dropIndex(String indexName) {
	long newIndexId = objectHolder.getRelationIdByRelationName(indexName);
	if (newIndexId != -1) {
	    Index newIndex = (Index) objectHolder.getObject(newIndexId);
	    bufferManager.writeRecordBitmap(newIndexId, newIndex.getPage(), newIndex.getRecordsPerPage(), newIndex.getRecord(), false);
	    bufferManager.closeFile(newIndexId);
	    bufferManager.deleteFile(newIndex.getFileName());
	    objectHolder.removeObject(newIndexId);
	    return true;
	}
	return false;
    }

    public boolean insertRecord(String query) {
	String relationName = query.split(" ")[2].trim();
	ObjectHolder objectHolder = ObjectHolder.getObjectHolder();
	long relationId = objectHolder.getRelationIdByRelationName(relationName);
	if (relationId != -1) {
	    Relation relation = (Relation) objectHolder.getObject(relationId);
	    long freePageNumber = bufferManager.getFreePage();
	    int recordOffset = bufferManager.getFreeRecordOffset(relationId, freePageNumber, relation.getRecordsPerPage(), relation.getRecordSize());
	    String[] columnList = query.substring(query.indexOf('(') + 1, query.indexOf(')')).split(",");
	    String[] valueList = query.substring(query.lastIndexOf('(') + 1, query.lastIndexOf(')')).split(",");
	    ByteBuffer serializedBuffer = Utility.serialize(columnList, valueList, relation.getAttributes(), relation.getRecordSize());
	    if (serializedBuffer != null) {
		bufferManager.write(relation.getRelationId(), freePageNumber, recordOffset, serializedBuffer);
		int recordNumber = (recordOffset - (relation.getRecordsPerPage() + 7) / 8) / relation.getRecordSize();
		bufferManager.writeRecordBitmap(relation.getRelationId(), freePageNumber, relation.getRecordsPerPage(), recordNumber, true);
		return true;
	    }
	}
	return false;
    }
}
