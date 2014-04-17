package databaseManager;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Vector;

public class Index {

	public static final int INDEX_NAME_LENGTH = 50;

	private int nKeys;
	private String indexName;
	private String fileName;
	private boolean duplicates;
	private long id;
	private long parentId;
	private PhysicalAddress rootPage;
	private int rootOffset;
	private long catalogPage;
	private int catalogRecordOffset;
	private long pageCount;
	private long recordsCount;
	private int recordSize;
	private int keySize;
	private long creationDate;
	private long lastModified;
	private Vector<Attribute> attributes;
	
	public Index(String _indexName, long _id, long _parentId, boolean _duplicates, Vector<Attribute> _attributes) {
		indexName = _indexName;
		fileName = _indexName + ".index";
		id = _id;
		parentId = _parentId;
		duplicates = _duplicates;
		creationDate = (new Date()).getTime();
		lastModified = (new Date()).getTime();
		pageCount = 1;
		recordsCount = 0;
		attributes = _attributes;
		keySize = 0;
		for(int i=0; i <attributes.size();i++){
			keySize += attributes.get(i).getAttributeSize();
		}
		nKeys = 64;
		while (true) {
			if ((DiskSpaceManager.PAGE_SIZE * 8 - 7) / (1 + 8 * (nKeys * (20 + keySize) + 25)) >= 1 || nKeys > 0) {
				break;
			} else {
				nKeys--;
			}
		}
		recordSize = nKeys * (keySize + 20) + 25;
	}

	public Index(ByteBuffer serializedBuffer) {
		indexName = "";
		for (int i = 0; i < INDEX_NAME_LENGTH; i++) {
			if (serializedBuffer.getChar(2 * i) != '\0') {
				indexName += serializedBuffer.getChar(2 * i);
			}
		}
		rootPage = new PhysicalAddress();
		serializedBuffer.position(INDEX_NAME_LENGTH * 2);
		id = serializedBuffer.getLong();
		parentId = serializedBuffer.getLong();
		nKeys = serializedBuffer.getInt();
		keySize = serializedBuffer.getInt();
		recordSize = serializedBuffer.getInt();
		rootPage.id = serializedBuffer.getLong();
		rootPage.offset = serializedBuffer.getLong();
		rootOffset = serializedBuffer.getInt();
		catalogPage = serializedBuffer.getLong();
		catalogRecordOffset = serializedBuffer.getInt();
		pageCount = serializedBuffer.getLong();
		recordsCount = serializedBuffer.getLong();
		creationDate = serializedBuffer.getLong();
		lastModified = serializedBuffer.getLong();
		fileName = indexName + ".index";
	}

	/**
	 * Let number of records per page = X, Let the recordSize = Y, then X*(1) +
	 * X*(8*Y) = PAGE_SIZE
	 * 
	 * @return
	 */
	public int getRecordsPerPage() {
		int numberOfRecords = (int) (DiskSpaceManager.PAGE_SIZE * 8 / (1 + 8 * recordSize));
		return numberOfRecords;
	}

	public long getIndexId() {
		return id;
	}

	public String getIndexName() {
		return indexName;
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

	public int getNumberOfKeys() {
		return nKeys;
	}

	public int getRecordSize() {
		return recordSize;
	}

	public int recordsPerBlock() {
		int numberOfRecords = (int) ((DiskSpaceManager.PAGE_SIZE * 8 - 7) / (1 + 8 * recordSize));
		return numberOfRecords;
	}

	public ByteBuffer serialize() {	
		
		ByteBuffer serializedBuffer = ByteBuffer.allocate((int) SystemCatalogManager.RELATION_RECORD_SIZE);
		for (int i = 0; i < INDEX_NAME_LENGTH; i++) {
			if (i < indexName.length()) {
				serializedBuffer.putChar(indexName.charAt(i));
			} else {
				serializedBuffer.putChar('\0');
			}
		}
		serializedBuffer.putLong(id);
		serializedBuffer.putLong(parentId);
		serializedBuffer.putInt(nKeys);
		serializedBuffer.putInt(keySize);
		serializedBuffer.putInt(recordSize);
		serializedBuffer.putLong(rootPage.id);
		serializedBuffer.putLong(rootPage.offset);
		serializedBuffer.putInt(rootOffset);
		serializedBuffer.putLong(catalogPage);
		serializedBuffer.putInt(catalogRecordOffset);
		serializedBuffer.putLong(pageCount);
		serializedBuffer.putLong(recordsCount);
		serializedBuffer.putLong(creationDate);
		serializedBuffer.putLong(lastModified);
		return serializedBuffer;
	}

	public void setIndexname(String _indexName) {
		indexName = _indexName;
		lastModified = (new Date()).getTime();
	}

	public void setRecordSize(int _recordSize) {
		recordSize = _recordSize;
	}

	public void setRoot(PhysicalAddress _physicalAddress, int _rootOffset) {
		rootPage = _physicalAddress;
		rootOffset = _rootOffset;
	}

	public boolean containsDuplicates() {
		return duplicates;
	}

	public void setAddress(long pageNumber, int _recordOffset) {
		catalogPage = pageNumber;
		catalogRecordOffset = _recordOffset;
	}

	public PhysicalAddress getRootPageAddress() {
		return rootPage;
	}

	public int getRootOffset() {
		return rootOffset;
	}

	public long getPage() {
		return catalogPage;
	}

	public int getRecordOffset() {
		return catalogRecordOffset;
	}

	public int getKeySize() {
		return keySize;
	}

	public Vector<Attribute> getAttributes() {
		return attributes;
	}

	public Object getParentId() {
		return parentId;
	}
}
