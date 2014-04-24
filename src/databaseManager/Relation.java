package databaseManager;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Vector;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Relation {

	public static final int RELATION_NAME_LENGTH = 50;

	private String relationName;
	private String fileName;
	private Vector<Attribute> attributes;
	private Map<String, Integer> attributesNames;
	private Set<Long> indices;
	private Vector<Attribute> primaryKey;
	private Map<Long, Vector<Long>> indexed;
	private long id;
	private long pageCount;
	private long recordsCount;
	private int recordSize;
	private long creationDate;
	private long lastModified;
	private PhysicalAddress storedAddress;

	public Relation(String _relationName, long _id) {
		relationName = _relationName;
		fileName = _relationName + ".db";
		id = _id;
		attributes = new Vector<Attribute>();
		attributesNames = new HashMap<String, Integer>();
		indices = new HashSet<Long>();
		indexed = new HashMap<Long, Vector<Long>>();
		primaryKey = new Vector<Attribute>();
		creationDate = (new Date()).getTime();
		lastModified = (new Date()).getTime();
		pageCount = 1;
		recordSize = 0;
		recordsCount = 0;
	}

	public Relation(ByteBuffer serializedBuffer) {
		relationName = "";
		for (int i = 0; i < RELATION_NAME_LENGTH; i++) {
			if (serializedBuffer.getChar(2 * i) != '\0') {
				relationName += serializedBuffer.getChar(2 * i);
			}
		}
		serializedBuffer.position(RELATION_NAME_LENGTH * 2);
		storedAddress = new PhysicalAddress();
		id = serializedBuffer.getLong();
		int attributesCount = serializedBuffer.getInt();
		recordSize = serializedBuffer.getInt();
		pageCount = serializedBuffer.getLong();
		recordsCount = serializedBuffer.getLong();
		creationDate = serializedBuffer.getLong();
		lastModified = serializedBuffer.getLong();
		storedAddress.id = serializedBuffer.getLong();
		storedAddress.pageNumber = serializedBuffer.getLong();
		storedAddress.pageOffset = serializedBuffer.getInt();
		fileName = relationName + ".db";
		attributes = new Vector<Attribute>(attributesCount);
		attributesNames = new HashMap<String, Integer>();
		indices = new HashSet<Long>();
		indexed = new HashMap<Long, Vector<Long>>();
		primaryKey = new Vector<Attribute>();
	}

	public boolean addAttribute(String attributeName, Attribute.Type type, long _id) {
		if (!attributesNames.containsKey(attributeName)) {
			Attribute newAttribute = new Attribute(attributeName, type, id, id);
			newAttribute.setPosition(attributes.size());
			attributes.add(newAttribute);
			attributesNames.put(attributeName, attributes.size() - 1);
			recordSize = recordSize + Attribute.Type.getSize(type);
			return true;
		}
		return false;
	}

	public boolean addAttribute(String attributeName, Attribute.Type type, long _id, int length) {
		if (!attributesNames.containsKey(attributeName)) {
			Attribute newAttribute = new Attribute(attributeName, type, id, id, length);
			newAttribute.setPosition(attributes.size());
			attributes.add(newAttribute);
			attributesNames.put(attributeName, attributes.size() - 1);
			recordSize = recordSize + length;
			return true;
		}
		return false;
	}

	public boolean addAttribute(Attribute attribute, boolean addToSize) {
		if (!attributesNames.containsKey(attribute.getName())) {
			if (attribute.getPosition() == -1) {
				attribute.setPosition(attributes.size());
			}
			attributes.add(attribute.getPosition(), attribute);
			attributesNames.put(attribute.getName(), attribute.getPosition());
			if (attribute.isPartPK()) {
				primaryKey.add(attribute);
			}
			if (addToSize) {
				recordSize = recordSize + attribute.getAttributeSize();
			}
			return true;
		}
		return false;
	}

	public boolean addIndex(Index index) {
		if (indices.contains(index.getId())) {
			return false;
		} else {
			indices.add(index.getId());
			Vector<Attribute> indexAttributes = index.getAttributes();
			for (int i = 0; i < indexAttributes.size(); i++) {
				if (indexed.containsKey(indexAttributes.get(i).getId())) {
					indexed.get(indexAttributes.get(i).getId()).add(index.getId());
				} else {
					Vector<Long> temp = new Vector<Long>();
					temp.add(index.getId());
					indexed.put(indexAttributes.get(i).getId(), temp);
				}
			}
			lastModified = (new Date()).getTime();
			return true;
		}
	}

	public boolean addRecord() {
		recordsCount++;
		return true;
	}

	/**
	 * Let number of records per page = X, Let the recordSize = Y, then X*(1) +
	 * X*(8*Y) = PAGE_SIZE
	 * 
	 * @return
	 */
	public int getRecordsPerPage() {
		int numberOfRecords = (int) ((DiskSpaceManager.PAGE_SIZE * 8 - 7) / (1 + 8 * recordSize));
		return numberOfRecords;
	}

	public long getId() {
		return id;
	}

	public int getAttributesCount() {
		return attributes.size();
	}

	public String getName() {
		return relationName;
	}

	public String getFileName() {
		return fileName;
	}

	public long getFileSize() {
		File file = new File(System.getProperty("user.dir") + "/" + fileName);
		return file.length();
	}

	public long getCreationDate() {
		return creationDate;
	}

	public int getRecordSize() {
		return recordSize;
	}

	public PhysicalAddress getAddress(){
		return storedAddress;
	}
	public int getRecordOffset() {
		return storedAddress.pageOffset;
	}

	public long getPageNumber() {
		return storedAddress.pageNumber;
	}

	public Vector<Attribute> getAttributes() {
		return attributes;
	}

	public ByteBuffer serialize() {
		ByteBuffer serializedBuffer = ByteBuffer.allocate((int) SystemCatalogManager.RELATION_RECORD_SIZE);
		for (int i = 0; i < RELATION_NAME_LENGTH; i++) {
			if (i < relationName.length()) {
				serializedBuffer.putChar(relationName.charAt(i));
			} else {
				serializedBuffer.putChar('\0');
			}
		}
		serializedBuffer.putLong(id);
		serializedBuffer.putInt(attributes.size());
		serializedBuffer.putInt(recordSize);
		serializedBuffer.putLong(pageCount);
		serializedBuffer.putLong(recordsCount);
		serializedBuffer.putLong(creationDate);
		serializedBuffer.putLong(lastModified);
		serializedBuffer.putLong(storedAddress.id);
		serializedBuffer.putLong(storedAddress.pageNumber);
		serializedBuffer.putInt(storedAddress.pageOffset);
		return serializedBuffer;
	}

	public void setName(String _relationName) {
		relationName = _relationName;
		lastModified = (new Date()).getTime();
	}

	public void setRecordSize(int _recordSize) {
		recordSize = _recordSize;
	}

	public long getRecordsCount() {
		return recordsCount;
	}

	public void setAddress(long id,long page, int offset) {
		storedAddress = new PhysicalAddress(id,page,offset);
	}

	public Map<String, Integer> getAttributesNames() {
		return attributesNames;
	}

	public Attribute getAttributeByName(String name) {
		int attrPos = attributesNames.get(name);
		Attribute attr = attributes.elementAt(attrPos);
		return attr;
	}

	public int getAttributePosition(String attrName) {
		if (attributesNames.containsKey(attrName)) {
			return attributesNames.get(attrName);
		}
		return -1;
	}

	public Attribute.Type getAttributeType(String field) {
		int attrPos = attributesNames.get(field);
		Attribute attr = attributes.elementAt(attrPos);
		return attr.getType();
	}

	public long updateRecordsCount(int i) {
		recordsCount = recordsCount + i;
		return recordsCount;
	}

	public Set<Long> getIndices() {
		return indices;
	}

	public void removeIndex(long indexId) {
		indices.remove(indexId);
		for (Map.Entry<Long, Vector<Long>> entry : indexed.entrySet()) {
			entry.getValue().remove(indexId);
		}
	}

	public void addToPrimaryKey(Attribute attr, boolean ordered) {
		primaryKey.add(attr);
	}

	public Vector<Attribute> getPrimaryKey() {
		return primaryKey;
	}

	public void addPrimaryKey(Vector<String> attrs) {
		for (int i = 0; i < attributes.size(); i++) {
			if (attrs.contains(attributes.get(i).getName())) {
				primaryKey.add(attributes.get(i));
			}
		}
	}

	public void setFileName(String _fileName) {
		fileName = _fileName;
	}
}
