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
	private Map< Long, Vector<Long> > indexed;
	private long id;
	private long pageCount;
	private long recordsCount;
	private int recordSize;
	private long creationDate;
	private long lastModified;
	private long pageNumber;
	private int recordOffset;

	public Relation(String _relationName, long _id) {
		relationName = _relationName;
		fileName = _relationName + ".db";
		id = _id;
		attributes = new Vector<Attribute>();
		attributesNames = new HashMap<String, Integer>();
		indices = new HashSet<Long>();
		indexed = new HashMap<Long,Vector<Long>>();
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
		id = serializedBuffer.getLong();
		int attributesCount = serializedBuffer.getInt();
		recordSize = serializedBuffer.getInt();
		pageCount = serializedBuffer.getLong();
		recordsCount = serializedBuffer.getLong();
		creationDate = serializedBuffer.getLong();
		lastModified = serializedBuffer.getLong();
		pageNumber = serializedBuffer.getLong();
		recordOffset = serializedBuffer.getInt();
		fileName = relationName + ".db";
		attributes = new Vector<Attribute>(attributesCount);
		attributesNames = new HashMap<String, Integer>();
		indices = new HashSet<Long>();
		indexed = new HashMap<Long,Vector<Long>>();
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
		if (!attributesNames.containsKey(attribute.getAttributeName())) {
			if (attribute.getPosition() == -1) {
				attribute.setPosition(attributes.size());
			}
			attributes.add(attribute.getPosition(), attribute);
			attributesNames.put(attribute.getAttributeName(), attribute.getPosition());
			if (addToSize) {

				recordSize = recordSize + attribute.getAttributeSize();
			}
			return true;
		}
		return false;
	}

	public boolean addIndex(Index index) {
		if (indices.contains(index.getIndexId())) {
			return false;
		} else {
			indices.add(index.getIndexId());
			Vector<Attribute> indexAttributes = index.getAttributes();
			for(int i=0; i<indexAttributes.size();i++){
				if(indexed.containsKey(indexAttributes.get(i).getId())){
					indexed.get(indexAttributes.get(i).getId()).add(index.getIndexId());
				}else{
					Vector<Long> temp = new Vector<Long>();
					temp.add(index.getIndexId());
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

	public long getRelationId() {
		return id;
	}

	public int getAttributesCount() {
		return attributes.size();
	}

	public String getRelationName() {
		return relationName;
	}

	public String getFileName() {
		return fileName;
	}

	public long getFileSize() {
		File file = new File(fileName);
		return file.length();
	}

	public long getCreationDate() {
		return creationDate;
	}

	public int getRecordSize() {
		return recordSize;
	}

	public int getRecordOffset() {
		return recordOffset;
	}

	public long getPageNumber() {
		return pageNumber;
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
		serializedBuffer.putLong(pageNumber);
		serializedBuffer.putInt(recordOffset);
		return serializedBuffer;
	}

	public void setRelationname(String _relationName) {
		relationName = _relationName;
		lastModified = (new Date()).getTime();
	}

	public void setRecordSize(int _recordSize) {
		recordSize = _recordSize;
	}

	public long getRecordsCount() {
		return recordsCount;
	}

	public void setAddress(long page, int offset) {
		pageNumber = page;
		recordOffset = offset;
	}

	public Map<String, Integer> getAttributesNames() {
		return attributesNames;
	}
	
	public Attribute getAttributeByName(String name){
    	int attrPos = attributesNames.get(name);
    	Attribute attr = attributes.elementAt(attrPos);
    	return attr;
    }

	public Attribute.Type getAttributeType(String field){
    	int attrPos = attributesNames.get(field);
    	Attribute attr = attributes.elementAt(attrPos);
    	return attr.getAttributeType();
    }
    
	public long updateRecordsCount(int i) {
		recordsCount = recordsCount + i;
		return recordsCount;
	}

}
