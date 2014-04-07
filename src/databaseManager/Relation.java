package databaseManager;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import databaseManager.Attribute.Type;

public class Relation {

	public static final int RELATION_NAME_LENGTH = 50;
	
	private String relationName;
	private String fileName;
	private ArrayList<Attribute> attributes;
	private Set<String> attributesNames;
	private Map<String, String> indexFiles;
	private Set<String> indexed;
	private long id;
	private long blockCount;
	private long recordsCount;
	private int recordSize;
	private long creationDate;
	private long lastModified;

	public Relation(String _relationName, long _id) {
		relationName = _relationName;
		fileName = _relationName + ".db";
		id = _id;
		attributes = new ArrayList<Attribute>();
		attributesNames = new HashSet<String>();
		indexFiles = new HashMap<String, String>();
		indexed = new HashSet<String>();
		creationDate = (new Date()).getTime();
		lastModified = (new Date()).getTime();
		blockCount = 1;
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
		recordSize = serializedBuffer.getInt();
		blockCount = serializedBuffer.getInt();
		recordsCount = serializedBuffer.getLong();
		creationDate = serializedBuffer.getLong();
		lastModified = serializedBuffer.getLong();
		fileName = relationName+".db";
		attributes = new ArrayList<Attribute>();
		attributesNames = new HashSet<String>();
		indexFiles = new HashMap<String, String>();
		indexed = new HashSet<String>();
	}

	public boolean addAttribute(String attributeName, Attribute.Type type,
			long _id) {
		if (!attributesNames.contains(attributeName)) {
			attributes.add(new Attribute(attributeName, type, id, id));
		}
		return false;
	}

	public boolean addAttribute(String attributeName, Attribute.Type type,
			long _id, int length) {
		if (!attributesNames.contains(attributeName)) {
			attributes.add(new Attribute(attributeName, type, id, id, length));
			recordSize = recordSize + length;
		}
		return false;
	}

	public boolean addIndex(String indexName) {
		if (indexFiles.containsKey(indexName)) {
			return false;
		} else {
			indexFiles
					.put(indexName, relationName + "_" + indexName + ".index");
			lastModified = (new Date()).getTime();
			return true;
		}
	}

	public boolean addRecord() {
		recordsCount++;
		return true;
	}

	/**
	 * Let number of records per block = X, Let the recordSize = Y, then X*(1) +
	 * X*(8*Y) = BLOCK_SIZE
	 * 
	 * @return
	 */
	public long getRecordsPerBlock() {
		long numberOfRecords = (int) (DiskSpaceManager.BLOCK_SIZE * 8 / (1 + 8 * recordSize));
		return numberOfRecords;
	}

	public long getRelationId() {
		return id;
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

	public long getRecordSize() {
		return recordSize;
	}

	public ArrayList<Attribute> getAttributes() {
		return attributes;
	}

	public ByteBuffer serialize() {
		ByteBuffer serializedBuffer = ByteBuffer
				.allocate((int) SystemCatalogManager.RELATION_RECORD_SIZE);
		for (int i = 0; i < RELATION_NAME_LENGTH; i++) {
			if (i < relationName.length()) {
				serializedBuffer.putChar(relationName.charAt(i));
			} else {
				serializedBuffer.putChar('\0');
			}
		}
		serializedBuffer.putLong(id);
		serializedBuffer.putInt(recordSize);
		serializedBuffer.putLong(blockCount);
		serializedBuffer.putLong(recordsCount);
		serializedBuffer.putLong(creationDate);
		serializedBuffer.putLong(lastModified);
		return serializedBuffer;
	}

	public void setRelationname(String _relationName) {
		relationName = _relationName;
		lastModified = (new Date()).getTime();
	}

	public void setRecordSize(int _recordSize) {
		recordSize = _recordSize;
	}
}
