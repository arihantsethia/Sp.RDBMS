/**
 * @author Arihant , Arun and Vishavdeep
 * Class SystemCatalogManager.java
 * This class access methods of class BufferManager. and it's methods will be accessed by DatabaseManager class. 
 *   
 */

package databaseManager;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
	public static final String INDEX_ATTRIBUTE_CATALOG = "index_attribute_catalog.db";
	public static final int RELATION_RECORD_SIZE = 160;
	public static final long RELATION_CATALOG_ID = 0;
	public static final int ATTRIBUTE_RECORD_SIZE = 142;
	public static final long ATTRIBUTE_CATALOG_ID = 1;
	public static final int INDEX_RECORD_SIZE = 192;
	public static final long INDEX_CATALOG_ID = 2;
	public static final int INDEX_ATTRIBUTE_RECORD_SIZE = 142;
	public static final long INDEX_ATTRIBUTE_CATALOG_ID = 3;

	private BufferManager bufferManager;
	private ObjectHolder objectHolder;
	public long totalAttributesCount;
	public long totalIndexAttributesCount;
	public long totalObjectsCount;

	public SystemCatalogManager() {
		bufferManager = BufferManager.getBufferManager();
		objectHolder = ObjectHolder.getObjectHolder();
		totalObjectsCount = 4;
		loadRelationCatalog();
		loadAttributeCatalog();
		loadIndexCatalog();
		loadIndexAttributeCatalog();
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
				if (totalObjectsCount <= tempRelation.getId()) {
					totalObjectsCount = tempRelation.getId() + 1;
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
				if (totalObjectsCount < tempIndex.getId()) {
					totalObjectsCount = tempIndex.getId() + 1;
				}
				objectHolder.addObject(tempIndex);
			}
		}
	}

	public void loadIndexAttributeCatalog() {
		Relation attributeRelation = new Relation("index_attribute_catalog", INDEX_ATTRIBUTE_CATALOG_ID);
		attributeRelation.setRecordSize(INDEX_ATTRIBUTE_RECORD_SIZE);
		objectHolder.addObject(attributeRelation);
		Set<Long> indexIds = new HashSet<Long>();
		Attribute tempAttribute;
		Iterator attributeIterator = new Iterator(attributeRelation);
		while (attributeIterator.hasNext()) {
			ByteBuffer attributeRecord = attributeIterator.getNext();
			if (attributeRecord != null) {
				attributeRecord.position(0);
				tempAttribute = new Attribute(attributeRecord);
				if (totalIndexAttributesCount <= tempAttribute.getId()) {
					totalIndexAttributesCount = tempAttribute.getId() + 1;
				}
				indexIds.add(tempAttribute.getParentId());
				objectHolder.addObjectToRelation(tempAttribute, false);
			}
		}
		java.util.Iterator<Long> iterator = indexIds.iterator();
		while(iterator.hasNext()){
			Index currIndex = (Index)objectHolder.getObject(iterator.next());
			if(currIndex.setTree()){
				updateIndexCatalog(currIndex);
			}
			objectHolder.addObjectToRelation(currIndex, false);
		}
	}

	public boolean createTable(String relationName, Vector<Vector<String>> parsedData) {
		if (objectHolder.getRelationId(relationName) == -1) {
			Relation newRelation = new Relation(relationName, totalObjectsCount);
			String attributeName;
			Attribute.Type attributeType;
			boolean nullable, distinct;
			int size;
			for (int i = 0; i < parsedData.size(); i++) {
				attributeName = parsedData.get(i).get(0);
				attributeType = Attribute.stringToType(parsedData.get(i).get(1));
				size = Integer.parseInt(parsedData.get(i).get(2));
				if (size == -1) {
					size = Attribute.Type.getSize(attributeType);
				}
				nullable = Boolean.valueOf(parsedData.get(i).get(3));
				distinct = Boolean.valueOf(parsedData.get(i).get(4));
				Attribute newAttribute = new Attribute(attributeName, attributeType, -1 , newRelation.getId(), size, nullable, distinct);
				if(!newRelation.addAttribute(newAttribute, true)){
					System.out.println("Table already contains " + attributeName + "! Duplicate entries not allowed.");
					return false;
				}
			}
			for (int i = 0; i < newRelation.getAttributesCount(); i++) {
				newRelation.getAttributes().get(i).setId(totalAttributesCount);
				addAttributeToCatalog(newRelation.getAttributes().get(i));					
			}			
			objectHolder.addObject(newRelation);
			addRelationToCatalog(newRelation);
			System.out.println("Table " + relationName + " successfully created!");
			return true;
		}
		System.out.println("Table " + relationName + " already exists!");
		return false;
	}

	public boolean createIndex(String indexName, String relationName, Vector<Vector<String>> parsedData) {
		if (objectHolder.getIndexId(relationName,indexName) == -1) {
			long relationId = objectHolder.getRelationId(relationName);
			if (relationId != -1) {
				Relation relation = (Relation) objectHolder.getObject(relationId);
				Vector<Attribute> attributes = new Vector<Attribute>();
				for (int i = 0; i < parsedData.get(0).size(); i++) {
					Attribute attribute = relation.getAttributeByName(parsedData.get(0).get(i));
					if(attribute!=null){
						attributes.add(relation.getAttributeByName(parsedData.get(0).get(i)));
					}else{
						System.out.println("Attribute : " + parsedData.get(0).get(i) + " doesn't exists!");
						return false;
					}
					
				}
				boolean distinct = Boolean.valueOf(parsedData.get(1).get(0));
				Index index = new Index(indexName,totalObjectsCount,relationId,distinct,attributes);
				for (int i = 0; i < attributes.size(); i++) {
					attributes.get(i).setParentId(totalObjectsCount);
					attributes.get(i).setId(totalIndexAttributesCount);
					addIndexAttributeToCatalog(attributes.get(i));					
				}
				objectHolder.addObject(index);
				index.setTree();
				objectHolder.addObjectToRelation(index,false);
				addIndexToCatalog(index);
				System.out.println("Index : " + indexName + " created successfully!");				
				//Add all records to index
				DynamicObject recordObject = new DynamicObject(relation.getAttributes());
				DynamicObject iteratorKey = new DynamicObject(attributes);
				PhysicalAddress recordAddress = new PhysicalAddress();
				recordAddress.id = relation.getId();
				Iterator iterator = new Iterator(relation);
				while (iterator.hasNext()) {
					ByteBuffer record = iterator.getNext();
					if (record != null) {
						record.position(0);
						recordObject =  recordObject.deserialize(record.array());
						for(int i=0;i<attributes.size();i++){
							iteratorKey.obj[i] = recordObject.obj[relation.getAttributePosition(attributes.get(i).getName())];
						}
						recordAddress.offset = iterator.currentPage;
						index.insert(iteratorKey, recordAddress, iterator.position - relation.getRecordSize());
					}
				}
				//Added all records to index				
				return true;
			} else {
				System.out.println("Relation : " + relationName + " doesn't exists!");
				return false;
			}
		}
		System.out.println("Index name already " + relationName + " already exists!");
		return false;
	}

	public void addAttributeToCatalog(Attribute newAttribute) {
		int attributeRecordsPerPage = (int) (DiskSpaceManager.PAGE_SIZE * 8 / (1 + 8 * ATTRIBUTE_RECORD_SIZE));
		long freePageNumber = bufferManager.getFreePageNumber(ATTRIBUTE_CATALOG_ID);
		int freeRecordOffset = bufferManager.getFreeRecordOffset(ATTRIBUTE_CATALOG_ID, freePageNumber, attributeRecordsPerPage, ATTRIBUTE_RECORD_SIZE);
		newAttribute.setAddress(freePageNumber, freeRecordOffset);
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
		newRelation.setAddress(freePageNumber, freeRecordOffset);
		bufferManager.write(RELATION_CATALOG_ID, freePageNumber, freeRecordOffset, newRelation.serialize());
		bufferManager.writeRecordBitmap(RELATION_CATALOG_ID, freePageNumber, relationRecordsPerPage, recordNumber, true);
		totalObjectsCount++;
	}

	public void addIndexAttributeToCatalog(Attribute newAttribute) {
		int attributeRecordsPerPage = (int) (DiskSpaceManager.PAGE_SIZE * 8 / (1 + 8 * INDEX_ATTRIBUTE_RECORD_SIZE));
		long freePageNumber = bufferManager.getFreePageNumber(INDEX_ATTRIBUTE_CATALOG_ID);
		int freeRecordOffset = bufferManager.getFreeRecordOffset(INDEX_ATTRIBUTE_CATALOG_ID, freePageNumber, attributeRecordsPerPage, INDEX_ATTRIBUTE_RECORD_SIZE);
		newAttribute.setAddress(freePageNumber, freeRecordOffset);
		bufferManager.write(INDEX_ATTRIBUTE_CATALOG_ID, freePageNumber, freeRecordOffset, newAttribute.serialize());
		int recordNumber = (freeRecordOffset - (attributeRecordsPerPage + 7) / 8) / INDEX_ATTRIBUTE_RECORD_SIZE;
		bufferManager.writeRecordBitmap(INDEX_ATTRIBUTE_CATALOG_ID, freePageNumber, attributeRecordsPerPage, recordNumber, true);
		totalIndexAttributesCount++;
	}

	public void addIndexToCatalog(Index newIndex) {
		int indexRecordsPerPage = (int) (DiskSpaceManager.PAGE_SIZE * 8 / (1 + 8 * INDEX_RECORD_SIZE));
		long freePageNumber = bufferManager.getFreePageNumber(INDEX_CATALOG_ID);
		int freeRecordOffset = bufferManager.getFreeRecordOffset(INDEX_CATALOG_ID, freePageNumber, indexRecordsPerPage, INDEX_RECORD_SIZE);
		int recordNumber = (freeRecordOffset - (indexRecordsPerPage + 7) / 8) / INDEX_RECORD_SIZE;
		newIndex.setAddress(freePageNumber, freeRecordOffset);
		bufferManager.write(INDEX_CATALOG_ID, freePageNumber, freeRecordOffset, newIndex.serialize());
		bufferManager.writeRecordBitmap(INDEX_CATALOG_ID, freePageNumber, indexRecordsPerPage, recordNumber, true);
		totalObjectsCount++;
	}

	public void updateRelationCatalog(Relation relation) {
		bufferManager.write(RELATION_CATALOG_ID, relation.getPageNumber(), relation.getRecordOffset(), relation.serialize());
	}
	
	public void updateIndexCatalog(Index index) {
		bufferManager.write(INDEX_CATALOG_ID, index.getPageNumber(), index.getRecordOffset(), index.serialize());
	}

	public void close() {
		bufferManager.flush();
	}

	public boolean dropTable(String relationName) {
		long relationId = objectHolder.getRelationId(relationName);
		int recordsPerPage, recordNumber;
		if (relationId != -1) {
			Relation relation = (Relation) objectHolder.getObject(relationId);
			recordsPerPage = (int) (DiskSpaceManager.PAGE_SIZE * 8 / (1 + 8 * ATTRIBUTE_RECORD_SIZE));
			Vector<Attribute> attributes = relation.getAttributes();
			for (int i = 0; i < attributes.size(); i++) {
				recordNumber = (attributes.get(i).getRecordOffset() - (recordsPerPage + 7) / 8) / ATTRIBUTE_RECORD_SIZE;
				bufferManager.writeRecordBitmap(ATTRIBUTE_CATALOG_ID, attributes.get(i).getPageNumber(), recordsPerPage, recordNumber, false);
			}
			java.util.Iterator<Long> indexIds = relation.getIndices().iterator();
			while (indexIds.hasNext()) {
				Index index = (Index) objectHolder.getObject(indexIds.next());
				dropIndex(index.getName(),relationName);
			}
			recordsPerPage = (int) (DiskSpaceManager.PAGE_SIZE * 8 / (1 + 8 * RELATION_RECORD_SIZE));
			recordNumber = (relation.getRecordOffset() - (recordsPerPage + 7) / 8) / RELATION_RECORD_SIZE;
			bufferManager.writeRecordBitmap(RELATION_CATALOG_ID, relation.getPageNumber(), recordsPerPage, recordNumber, false);
			bufferManager.closeFile(relationId);
			bufferManager.deleteFile(relation.getFileName());
			objectHolder.removeObject(relationId);
			return true;
		}
		return false;
	}

	public boolean dropIndex(String indexName, String relationName) {
		long indexId = objectHolder.getIndexId(relationName,indexName);
		int indexRecordsPerPage = (int) (DiskSpaceManager.PAGE_SIZE * 8 / (1 + 8 * INDEX_RECORD_SIZE));
		if (indexId != -1) {
			long relationId =  objectHolder.getRelationId(relationName);
			Relation relation = (Relation) objectHolder.getObject(relationId);
			Index index = (Index) objectHolder.getObject(indexId);
			relation.removeIndex(indexId);
			int recordNumber = (index.getRecordOffset() - (indexRecordsPerPage + 7) / 8) / RELATION_RECORD_SIZE;
			bufferManager.writeRecordBitmap(INDEX_CATALOG_ID, index.getPageNumber(), index.getRecordsPerPage(), recordNumber, false);
			bufferManager.closeFile(indexId);
			bufferManager.deleteFile(index.getFileName());
			objectHolder.removeObject(indexId);
			return true;
		}
		return false;
	}

	public boolean insertRecord(String query) {
		String relationName = query.split(" ")[2].trim();
		ObjectHolder objectHolder = ObjectHolder.getObjectHolder();
		long relationId = objectHolder.getRelationId(relationName);
		if (relationId != -1) {
			Relation relation = (Relation) objectHolder.getObject(relationId);
			long freePageNumber = bufferManager.getFreePageNumber(relationId);
			int recordOffset = bufferManager.getFreeRecordOffset(relationId, freePageNumber, relation.getRecordsPerPage(), relation.getRecordSize());
			String[] columnList = query.substring(query.indexOf('(') + 1, query.indexOf(')')).split(",");
			String[] valueList = query.substring(query.lastIndexOf('(') + 1, query.lastIndexOf(')')).split(",");
			ByteBuffer serializedBuffer = Utility.serialize(columnList, valueList, relation.getAttributes(), relation.getRecordSize());
			if (serializedBuffer != null) {
				bufferManager.write(relation.getId(), freePageNumber, recordOffset, serializedBuffer);
				int recordNumber = (recordOffset - (relation.getRecordsPerPage() + 7) / 8) / relation.getRecordSize();
				bufferManager.writeRecordBitmap(relation.getId(), freePageNumber, relation.getRecordsPerPage(), recordNumber, true);
				relation.updateRecordsCount(1);
				java.util.Iterator<Long> indices = relation.getIndices().iterator();
				PhysicalAddress insertAddress = new PhysicalAddress(relationId,freePageNumber); 
				while(indices.hasNext()){
					Index currIndex = ((Index)objectHolder.getObject(indices.next()));
					if(currIndex.setTree()){
						updateIndexCatalog(currIndex);
					}
					DynamicObject entryObject = Utility.toDynamicObject(columnList, valueList, currIndex.getAttributes());
					currIndex.insert(entryObject, insertAddress , recordOffset);
				}
				updateRelationCatalog(relation);
				return true;
			}
		}
		return false;
	}

	public boolean showTables() {
		for (Map.Entry<Long, Object> entry : objectHolder.objects.entrySet()) {
			if ((entry.getValue() instanceof Relation) && entry.getKey() > 2) {
				System.out.println(((Relation) entry.getValue()).getName());
			}
		}
		return true;
	}
}
