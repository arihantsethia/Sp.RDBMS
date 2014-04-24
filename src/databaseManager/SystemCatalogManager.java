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

import queriesManager.QueryParser;

public class SystemCatalogManager {

	public static final String RELATION_CATALOG = "relation_catalog.db";
	public static final String ATTRIBUTE_CATALOG = "attribute_catalog.db";
	public static final String INDEX_CATALOG = "index_catalog.db";
	public static final String INDEX_ATTRIBUTE_CATALOG = "index_attribute_catalog.db";
	public static final int RELATION_RECORD_SIZE = 168;
	public static final long RELATION_CATALOG_ID = 1;
	public static final int ATTRIBUTE_RECORD_SIZE = 151;
	public static final long ATTRIBUTE_CATALOG_ID = 2;
	public static final int INDEX_RECORD_SIZE = 204;
	public static final long INDEX_CATALOG_ID = 3;
	public static final int INDEX_ATTRIBUTE_RECORD_SIZE = 151;
	public static final long INDEX_ATTRIBUTE_CATALOG_ID = 4;

	private BufferManager bufferManager;
	private ObjectHolder objectHolder;
	public long totalAttributesCount;
	public long totalIndexAttributesCount;
	public long totalObjectsCount;

	public SystemCatalogManager() {
		bufferManager = BufferManager.getBufferManager();
		objectHolder = ObjectHolder.getObjectHolder();
		objectHolder.clearAll();
		totalAttributesCount = 0;
		totalIndexAttributesCount = 0;
		totalObjectsCount = 5;
		loadRelationCatalog();
		loadAttributeCatalog();
		loadIndexCatalog();
		loadIndexAttributeCatalog();
	}

	/**
	 * Loads all the relationObjects of current database
	 */
	public void loadRelationCatalog() {
		Relation tableRelation = new Relation("relation_catalog", RELATION_CATALOG_ID);
		tableRelation.setRecordSize(RELATION_RECORD_SIZE);
		objectHolder.addObject(tableRelation);
		Relation tempRelation;
		Iterator relationIterator = new Iterator(tableRelation);
		while (relationIterator.hasNext()) {
			ByteBuffer relationRecord = relationIterator.getNext();
			if (relationRecord != null) {
				bufferManager.pinPage(RELATION_CATALOG_ID, relationIterator.currentPage);
				relationRecord.position(0);
				tempRelation = new Relation(relationRecord);
				if (totalObjectsCount <= tempRelation.getId()) {
					totalObjectsCount = tempRelation.getId() + 1;
				}
				objectHolder.addObject(tempRelation);
			}
		}
	}

	/**
	 * Loads all the attributes of all relation in current database
	 */
	public void loadAttributeCatalog() {
		Relation attributeRelation = new Relation("attribute_catalog", ATTRIBUTE_CATALOG_ID);
		attributeRelation.setRecordSize(ATTRIBUTE_RECORD_SIZE);
		objectHolder.addObject(attributeRelation);
		Attribute tempAttribute;
		Iterator attributeIterator = new Iterator(attributeRelation);
		while (attributeIterator.hasNext()) {
			ByteBuffer attributeRecord = attributeIterator.getNext();
			if (attributeRecord != null) {
				bufferManager.pinPage(ATTRIBUTE_CATALOG_ID, attributeIterator.currentPage);
				attributeRecord.position(0);
				tempAttribute = new Attribute(attributeRecord);
				if (totalAttributesCount <= tempAttribute.getId()) {
					totalAttributesCount = tempAttribute.getId() + 1;
				}
				objectHolder.addObjectToRelation(tempAttribute, false);
			}
		}
	}

	/**
	 * Loads all the index of all relation in current database
	 */
	public void loadIndexCatalog() {
		Relation indexRelation = new Relation("index_catalog", INDEX_CATALOG_ID);
		indexRelation.setRecordSize(INDEX_RECORD_SIZE);
		objectHolder.addObject(indexRelation);
		Index tempIndex;
		Iterator indexIterator = new Iterator(indexRelation);
		while (indexIterator.hasNext()) {
			ByteBuffer indexRecord = indexIterator.getNext();
			if (indexRecord != null) {
				bufferManager.pinPage(INDEX_CATALOG_ID, indexIterator.currentPage);
				indexRecord.position(0);
				tempIndex = new Index(indexRecord);
				if (totalObjectsCount < tempIndex.getId()) {
					totalObjectsCount = tempIndex.getId() + 1;
				}
				objectHolder.addObject(tempIndex);
			}
		}
	}

	/**
	 * Loads all the attributes of all indexes in current database
	 */
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
				bufferManager.pinPage(INDEX_ATTRIBUTE_CATALOG_ID, attributeIterator.currentPage);
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
		while (iterator.hasNext()) {
			Index currIndex = (Index) objectHolder.getObject(iterator.next());
			if (currIndex.setTree()) {
				updateIndexCatalog(currIndex);
			}
			objectHolder.addObjectToRelation(currIndex, false);
		}
	}

	/**
	 * Writes the attribute to the attribute_catalog.db
	 * 
	 * @param newAttribute
	 *            : Object of attribute that should be inserted to catalog
	 */
	public void addAttributeToCatalog(Attribute newAttribute) {
		int attributeRecordsPerPage = (int) (DiskSpaceManager.PAGE_SIZE * 8 / (1 + 8 * ATTRIBUTE_RECORD_SIZE));
		long freePageNumber = bufferManager.getFreePageNumber(ATTRIBUTE_CATALOG_ID);
		int freeRecordOffset = bufferManager.getFreeRecordOffset(ATTRIBUTE_CATALOG_ID, freePageNumber, attributeRecordsPerPage, ATTRIBUTE_RECORD_SIZE);
		newAttribute.setAddress(ATTRIBUTE_CATALOG_ID, freePageNumber, freeRecordOffset);
		bufferManager.write(ATTRIBUTE_CATALOG_ID, freePageNumber, freeRecordOffset, newAttribute.serialize());
		int recordNumber = (freeRecordOffset - (attributeRecordsPerPage + 7) / 8) / ATTRIBUTE_RECORD_SIZE;
		bufferManager.writeRecordBitmap(ATTRIBUTE_CATALOG_ID, freePageNumber, attributeRecordsPerPage, recordNumber, true);
		totalAttributesCount++;
	}

	/**
	 * Writes the relation to the relation_catalog.db
	 * 
	 * @param newRelation
	 *            : Object of relation that should be inserted to catalog
	 */
	public void addRelationToCatalog(Relation newRelation) {
		int relationRecordsPerPage = (int) (DiskSpaceManager.PAGE_SIZE * 8 / (1 + 8 * RELATION_RECORD_SIZE));
		long freePageNumber = bufferManager.getFreePageNumber(RELATION_CATALOG_ID);
		int freeRecordOffset = bufferManager.getFreeRecordOffset(RELATION_CATALOG_ID, freePageNumber, relationRecordsPerPage, RELATION_RECORD_SIZE);
		int recordNumber = (freeRecordOffset - (relationRecordsPerPage + 7) / 8) / RELATION_RECORD_SIZE;
		newRelation.setAddress(RELATION_CATALOG_ID, freePageNumber, freeRecordOffset);
		bufferManager.write(RELATION_CATALOG_ID, freePageNumber, freeRecordOffset, newRelation.serialize());
		bufferManager.writeRecordBitmap(RELATION_CATALOG_ID, freePageNumber, relationRecordsPerPage, recordNumber, true);
		totalObjectsCount++;
	}

	/**
	 * Writes the relation to the index_attribute_catalog.db
	 * 
	 * @param newAttribtue
	 *            : Object of attribute that should be inserted to catalog
	 */
	public void addIndexAttributeToCatalog(Attribute newAttribute) {
		int attributeRecordsPerPage = (int) (DiskSpaceManager.PAGE_SIZE * 8 / (1 + 8 * INDEX_ATTRIBUTE_RECORD_SIZE));
		long freePageNumber = bufferManager.getFreePageNumber(INDEX_ATTRIBUTE_CATALOG_ID);
		int freeRecordOffset = bufferManager.getFreeRecordOffset(INDEX_ATTRIBUTE_CATALOG_ID, freePageNumber, attributeRecordsPerPage, INDEX_ATTRIBUTE_RECORD_SIZE);
		newAttribute.setAddress(INDEX_ATTRIBUTE_CATALOG_ID, freePageNumber, freeRecordOffset);
		bufferManager.write(INDEX_ATTRIBUTE_CATALOG_ID, freePageNumber, freeRecordOffset, newAttribute.serialize());
		int recordNumber = (freeRecordOffset - (attributeRecordsPerPage + 7) / 8) / INDEX_ATTRIBUTE_RECORD_SIZE;
		bufferManager.writeRecordBitmap(INDEX_ATTRIBUTE_CATALOG_ID, freePageNumber, attributeRecordsPerPage, recordNumber, true);
		totalIndexAttributesCount++;
	}

	/**
	 * Writes the relation to the index_catalog.db
	 * 
	 * @param newAttribtue
	 *            : Object of index that should be inserted to catalog
	 */
	public void addIndexToCatalog(Index newIndex) {
		int indexRecordsPerPage = (int) (DiskSpaceManager.PAGE_SIZE * 8 / (1 + 8 * INDEX_RECORD_SIZE));
		long freePageNumber = bufferManager.getFreePageNumber(INDEX_CATALOG_ID);
		int freeRecordOffset = bufferManager.getFreeRecordOffset(INDEX_CATALOG_ID, freePageNumber, indexRecordsPerPage, INDEX_RECORD_SIZE);
		int recordNumber = (freeRecordOffset - (indexRecordsPerPage + 7) / 8) / INDEX_RECORD_SIZE;
		newIndex.setAddress(INDEX_CATALOG_ID, freePageNumber, freeRecordOffset);
		bufferManager.write(INDEX_CATALOG_ID, freePageNumber, freeRecordOffset, newIndex.serialize());
		bufferManager.writeRecordBitmap(INDEX_CATALOG_ID, freePageNumber, indexRecordsPerPage, recordNumber, true);
		totalObjectsCount++;
	}

	/**
	 * This function creates a new table in current database
	 * 
	 * @param relationName
	 *            : Name of the relation to be created
	 * @param parsedData
	 *            : This contains information about attributes of the relation
	 * @return : Returns true if table is successfully created
	 */
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
				Attribute newAttribute = new Attribute(attributeName, attributeType, -1, newRelation.getId(), size, nullable, distinct);
				if (!newRelation.addAttribute(newAttribute, true)) {
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
			for (int i = 0; i < newRelation.getAttributesCount(); i++) {
				Attribute newAttribute = newRelation.getAttributes().get(i);
				if (newAttribute.isDistinct()) {
					Vector<Vector<String>> data = new Vector<Vector<String>>();
					Vector<String> params = new Vector<String>();
					params.add(newAttribute.getName());
					data.add(params);
					params = new Vector<String>();
					params.add("true");
					data.add(params);
					createIndex(newAttribute.getName() + "_uk", relationName, data);
				}
			}
			System.out.println("Table " + relationName + " successfully created!");
			return true;
		}
		System.out.println("Table " + relationName + " already exists!");
		return false;
	}

	/**
	 * This function creates a new index on give table in current database
	 * 
	 * @param indexName
	 *            : Name of the index to be created
	 * @param relationName
	 *            : Name of the relation to which index should be added.
	 * @param parsedData
	 *            : This contains information about attributes of the relation
	 * @return : Returns true if index is successfully created
	 */
	public boolean createIndex(String indexName, String relationName, Vector<Vector<String>> parsedData) {
		if (objectHolder.getIndexId(relationName, indexName) == -1) {
			long relationId = objectHolder.getRelationId(relationName);
			if (relationId != -1) {
				Relation relation = (Relation) objectHolder.getObject(relationId);
				Vector<Attribute> attributes = new Vector<Attribute>();
				for (int i = 0; i < parsedData.get(0).size(); i++) {
					Attribute attribute = relation.getAttributeByName(parsedData.get(0).get(i));
					if (attribute != null) {
						attribute = new Attribute(relation.getAttributeByName(parsedData.get(0).get(i)).serialize());
						attributes.add(attribute);
					} else {
						System.out.println("Attribute : " + parsedData.get(0).get(i) + " doesn't exists!");
						return false;
					}

				}
				boolean distinct = Boolean.valueOf(parsedData.get(1).get(0));
				Index index = new Index(indexName, totalObjectsCount, relationId, distinct, attributes);
				for (int i = 0; i < attributes.size(); i++) {
					attributes.get(i).setParentId(totalObjectsCount);
					attributes.get(i).setId(totalIndexAttributesCount);
					addIndexAttributeToCatalog(attributes.get(i));
				}
				objectHolder.addObject(index);
				objectHolder.addObjectToRelation(index, false);
				updateRelationCatalog(relation);
				addIndexToCatalog(index);
				index.setTree();
				System.out.println("Index : " + indexName + " created successfully!");
				// Add all records to index
				DynamicObject recordObject = new DynamicObject(relation.getAttributes());
				DynamicObject iteratorKey = new DynamicObject(attributes);
				PhysicalAddress recordAddress = new PhysicalAddress();
				Iterator iterator = new Iterator(relation);
				while (iterator.hasNext()) {
					ByteBuffer record = iterator.getNext();
					if (record != null) {
						record.position(0);
						recordObject = recordObject.deserialize(record.array());
						for (int i = 0; i < attributes.size(); i++) {
							iteratorKey.obj[i] = recordObject.obj[relation.getAttributePosition(attributes.get(i).getName())];
						}
						recordAddress.id = relation.getId();
						recordAddress.pageNumber = iterator.currentPage;
						recordAddress.pageOffset = iterator.position - relation.getRecordSize();
						if (!index.insert(iteratorKey, recordAddress)) {
							dropIndex(indexName, relationName);
							return false;
						}
					}
				}
				return true;
			} else {
				System.out.println("Relation : " + relationName + " doesn't exists!");
				return false;
			}
		}
		System.out.println("Index name already " + relationName + " already exists!");
		return false;
	}

	/**
	 * This function drops the table in current database
	 * 
	 * @param relationName
	 *            : Name of the table which needs to be dropped.
	 * @return : Returns true if table was successfully dropped.
	 */
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
				dropIndex(index.getName(), relationName);
			}
			recordsPerPage = (int) (DiskSpaceManager.PAGE_SIZE * 8 / (1 + 8 * RELATION_RECORD_SIZE));
			recordNumber = (relation.getRecordOffset() - (recordsPerPage + 7) / 8) / RELATION_RECORD_SIZE;
			deleteRecord(RELATION_CATALOG_ID, relation.getPageNumber(), recordsPerPage, recordNumber);
			bufferManager.closeFile(relationId);
			bufferManager.deleteFile(relation.getFileName());
			objectHolder.removeObject(relationId);
			return true;
		}
		return false;
	}

	/**
	 * This function drops an index on give table in current database
	 * 
	 * @param indexName
	 *            : Name of the index which needs to be dropped.
	 * @param relationName
	 *            : Name of the relation from which index needs to be dropped.
	 * @return : Returns true if table was successfully dropped.
	 */
	public boolean dropIndex(String indexName, String relationName) {
		long indexId = objectHolder.getIndexId(relationName, indexName);
		int indexRecordsPerPage = (int) (DiskSpaceManager.PAGE_SIZE * 8 / (1 + 8 * INDEX_RECORD_SIZE));
		if (indexId != -1) {
			long relationId = objectHolder.getRelationId(relationName);
			Relation relation = (Relation) objectHolder.getObject(relationId);
			Index index = (Index) objectHolder.getObject(indexId);
			relation.removeIndex(indexId);
			int recordNumber = (index.getRecordOffset() - (indexRecordsPerPage + 7) / 8) / RELATION_RECORD_SIZE;
			deleteRecord(INDEX_CATALOG_ID, index.getPageNumber(), index.getRecordsPerPage(), recordNumber);
			bufferManager.closeFile(indexId);
			bufferManager.deleteFile(index.getFileName());
			objectHolder.removeObject(indexId);
			return true;
		}
		return false;
	}

	/**
	 * This function drops an index on give table in current database
	 * 
	 * @param query
	 *            : Name of the index which needs to be dropped.
	 * @param relationName
	 *            : Name of the relation from which index needs to be dropped.
	 * @return : Returns true if table was successfully dropped.
	 */
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
				java.util.Iterator<Long> indices = relation.getIndices().iterator();
				PhysicalAddress insertAddress = new PhysicalAddress(relationId, freePageNumber, recordOffset);
				while (indices.hasNext()) {
					Index currIndex = ((Index) objectHolder.getObject(indices.next()));
					if (currIndex.setTree()) {
						updateIndexCatalog(currIndex);
					}
					DynamicObject entryObject = Utility.toDynamicObject(columnList, valueList, currIndex.getAttributes());
					if (!currIndex.insert(entryObject, insertAddress)) {
						System.out.println("Couldn't insert into table. Doesn't satisfy the check constraints.");
						return false;
					}
				}
				bufferManager.writeRecordBitmap(relation.getId(), freePageNumber, relation.getRecordsPerPage(), recordNumber, true);
				relation.updateRecordsCount(1);
				updateRelationCatalog(relation);
				return true;
			}
		}
		return false;
	}

	/**
	 * Adds primary key to table if no primary key exists on the table
	 * 
	 * @param relationName
	 *            : Name of the table on which primary key needs to be created
	 * @param attrs
	 *            : Vector of attributes for the given key.
	 * @return true if successfully added primary key, else false;
	 */
	public boolean addPrimaryKey(String relationName, Vector<String> attrs) {
		long relationId = objectHolder.getRelationId(relationName);
		if (relationId != -1) {
			Relation relation = (Relation) objectHolder.getObject(relationId);
			if (relation.getPrimaryKey().size() == 0) {
				for (int i = 0; i < attrs.size(); i++) {
					Attribute attribute = relation.getAttributeByName(attrs.get(i));
					attribute.partPK(true);
					updateAttributeCatalog(attribute);
				}
				Vector<Vector<String>> data = new Vector<Vector<String>>();
				Vector<String> params = new Vector<String>();
				params.add("true");
				data.add(attrs);
				data.add(params);
				if (!createIndex(relationName + "_pk", relationName, data)) {
					System.out.println("Contains duplicate data. Cannot create primary key!");
					for (int i = 0; i < attrs.size(); i++) {
						Attribute attribute = relation.getAttributeByName(attrs.get(i));
						attribute.partPK(false);
						updateAttributeCatalog(attribute);
					}
					return false;
				}
				relation.addPrimaryKey(attrs);
				return true;
			} else {
				System.out.println("Table already has a primary key defined on it!");
				return false;
			}
		}
		return false;
	}

	public boolean deleteRecord(long id, long pageNumber, int recordNumber, int recordsPerPage) {
		return bufferManager.writeRecordBitmap(id, pageNumber, recordsPerPage, recordNumber, false);
	}

	/**
	 * Deletes the record from given relation by setting corresponding bit in
	 * bitmap to false
	 * 
	 * @param id
	 *            : relation id from which record needs to be deleted
	 * @param pageNumber
	 *            : page number from which record needs to be deleted
	 * @param recordNumber
	 *            : record number on page for the record.
	 * @param recordsPerPage
	 *            : Number of records per page for the relation from which
	 *            record is to be deleted.
	 * @return
	 */
	public boolean deleteRecord(long id, long pageNumber, int recordOffset, DynamicObject dObj) {
		Relation relation = (Relation) objectHolder.getObject(id);
		relation.updateRecordsCount(-1);
		java.util.Iterator<Long> indices = relation.getIndices().iterator();
		PhysicalAddress deleteAddress = new PhysicalAddress(id, pageNumber, recordOffset);
		int recordsPerPage = relation.getRecordsPerPage();
		int recordNumber = (recordOffset - (recordsPerPage + 7) / 8) / relation.getRecordSize();
		while (indices.hasNext()) {
			Index currIndex = ((Index) objectHolder.getObject(indices.next()));
			deleteIndexRecord(currIndex, dObj, deleteAddress);
		}
		relation.updateRecordsCount(-1);
		updateRelationCatalog(relation);
		return bufferManager.writeRecordBitmap(id, pageNumber, recordsPerPage, recordNumber, false);
	}

	private boolean deleteIndexRecord(Index index, DynamicObject dObj, PhysicalAddress deleteAddress) {
		if (index.setTree()) {
			updateIndexCatalog(index);
		}
		DynamicObject entryObject = new DynamicObject(index.getAttributes());
		for (int i = 0; i < entryObject.attributes.size(); i++) {
			for (int j = 0; j < index.getAttributes().size(); j++) {
				if (index.getAttributes().get(j).getName().equals(entryObject.attributes.get(i).getName())) {
					entryObject.obj[i] = dObj.obj[j];
					break;
				}
			}
		}
		return index.delete(entryObject, deleteAddress);
	}

	/**
	 * Updates the relation entry in relation catalog
	 * 
	 * @param relation
	 *            : relation for which update is to be done
	 */
	public void updateRelationCatalog(Relation relation) {
		bufferManager.write(RELATION_CATALOG_ID, relation.getPageNumber(), relation.getRecordOffset(), relation.serialize());
	}

	/**
	 * Updates the attribute entry in attribute catalog
	 * 
	 * @param attribute
	 *            : attribute for which update is to be done
	 */
	public void updateAttributeCatalog(Attribute attribute) {
		bufferManager.write(ATTRIBUTE_CATALOG_ID, attribute.getPageNumber(), attribute.getRecordOffset(), attribute.serialize());
	}

	/**
	 * Updates the index entry in index catalog
	 * 
	 * @param index
	 *            : index for which update is to be done
	 */
	public void updateIndexCatalog(Index index) {
		bufferManager.write(INDEX_CATALOG_ID, index.getPageNumber(), index.getRecordOffset(), index.serialize());
	}

	/**
	 * Updates the record from given relation
	 * 
	 * @param id
	 *            : relation id from which record needs to be updated
	 * @param pageNumber
	 *            : page number from which record needs to be updated
	 * @param recordOffset
	 *            : record offset on page for the record.
	 * @param serialBuffer
	 *            : Bytebuffer for the record.
	 */
	public void updateRecord(long id, long pageNumber, int recordOffset, ByteBuffer serialBuffer) {
		serialBuffer.position(0);
		bufferManager.write(id, pageNumber, recordOffset, serialBuffer);
	}

	public void updateRecord(Relation relation, long pageNumber, int recordOffset, DynamicObject dObj) {
		java.util.Iterator<Long> indices = relation.getIndices().iterator();
		PhysicalAddress address = new PhysicalAddress(relation.getId(), pageNumber, recordOffset);
		while (indices.hasNext()) {
			Index currIndex = ((Index) objectHolder.getObject(indices.next()));
			deleteIndexRecord(currIndex, dObj, address);
			DynamicObject entryObject = new DynamicObject(currIndex.getAttributes());
			for (int i = 0; i < entryObject.attributes.size(); i++) {
				for (int j = 0; j < currIndex.getAttributes().size(); j++) {
					if (currIndex.getAttributes().get(j).getName().equals(entryObject.attributes.get(i).getName())) {
						entryObject.obj[i] = dObj.obj[j];
						break;
					}
				}
			}
			currIndex.insert(entryObject, address);
		}
		ByteBuffer serialBuffer = dObj.serialize(dObj);
		serialBuffer.position(0);
		bufferManager.write(relation.getId(), pageNumber, recordOffset, serialBuffer);
	}

	/**
	 * Call this to ensure all pages in main memory are written back to physical
	 * memory. This function also resets the bufferManager
	 */
	public void close() {
		bufferManager.flush();
		bufferManager.unPinAll();
		bufferManager.initializeTable();
	}

	/**
	 * Shows the list of tables in current database
	 * 
	 * @return
	 */
	public boolean showTables() {
		int count = 0;
		String s = "";
		for (Map.Entry<Long, Object> entry : objectHolder.objects.entrySet()) {
			if ((entry.getValue() instanceof Relation) && entry.getKey() > 4) {
				count += 1;
				s = s + count + ".\t" + ((Relation) entry.getValue()).getName() + "\n";
			}
		}
		if (count == 0) {
			System.out.println("Empty set");
		} else {

			count = s.length() + 4 * count;
			while (count != 0) {
				System.out.print("-");
				count--;
			}
			System.out.println();
			System.out.println(s);
		}
		return true;
	}

	/**
	 * Prints description of the give table;
	 * 
	 * @param statement
	 *            : query in proper format. format: 'desc table table_name'
	 * @return true is query correctly formatted.
	 */
	boolean descOperation(String statement) {
		int index = statement.indexOf("table");
		if (index != -1) {
			statement = statement.substring(index + 5).trim();
			if (!statement.contains(" ")) {
				String relationName = statement;
				long relationId = ObjectHolder.getObjectHolder().getRelationId(relationName);
				if (relationId != -1) {
					Relation relation = (Relation) ObjectHolder.getObjectHolder().getObject(relationId);
					int length = 28;
					System.out.println("Table Description :-");
					for (int i = 0; i < length; i++) {
						System.out.print("-");
					}
					System.out.println();
					for (int i = 0; i < relation.getAttributesCount(); i++) {
						System.out.printf("%-12s | ", relation.getAttributes().get(i).getName());
						if (Attribute.Type.toString(relation.getAttributes().get(i).getType()).equals("int")) {
							System.out.printf("%-12s | \n", Attribute.Type.toString(relation.getAttributes().get(i).getType()));
						} else {
							System.out.printf("%-12s | \n", Attribute.Type.toString(relation.getAttributes().get(i).getType()) + "("
									+ (relation.getAttributes().get(i).getAttributeSize() + 1) / 2 + ")");
						}
					}
					if (relation.getPrimaryKey().size() > 0) {
						for (int i = 0; i < length; i++) {
							System.out.print("-");
						}
						System.out.println();
						System.out.print("Primary Key : ( ");
						for (int i = 0; i < relation.getPrimaryKey().size(); i++) {
							System.out.print(relation.getPrimaryKey().get(i).getName());
							if (i != relation.getPrimaryKey().size() - 1) {
								System.out.print(" , ");
							} else {
								System.out.println(" )");
							}
						}
					}
					if (relation.getIndices().size() > 0) {
						for (int i = 0; i < length; i++) {
							System.out.print("-");
						}
						System.out.println();
						int pos = 1;
						System.out.println("Number of indices are : " + relation.getIndices().size());
						java.util.Iterator<Long> indexIterator = relation.getIndices().iterator();
						while (indexIterator.hasNext()) {
							Index currIndex = (Index) objectHolder.getObject(indexIterator.next());
							System.out.print((pos++) + ". " + currIndex.getName() + " ( ");
							for (int i = 0; i < currIndex.getAttributes().size(); i++) {
								System.out.print(currIndex.getAttributes().get(i).getName());
								if (i != currIndex.getAttributes().size() - 1) {
									System.out.print(" , ");
								} else {
									System.out.println(" )");
								}
							}
						}
					}
					for (int i = 0; i < length; i++) {
						System.out.print("-");
					}
					System.out.println();
					return true;
				} else {
					QueryParser.print_error(2, relationName);
					return false;
				}
			} else {
				QueryParser.print_error(2, statement);
				return false;
			}
		}
		QueryParser.print_error(9, "");
		return false;
	}
}
