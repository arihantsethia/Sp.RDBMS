package databaseManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Relation {

	private String relationName;
	private String fileName;
	private ArrayList<Attribute> attributes;
	private Set<String> attributesNames;
	private Map<String, String> indexFiles;
	private Set<String> indexed;
	private long id;
	private int blockCount;
	private int recordsCount;
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
		// TODO Auto-generated method stub
		return null;
	}

	public void setRelationname(String _relationName) {
		relationName = _relationName;
		lastModified = (new Date()).getTime();
	}	
}
