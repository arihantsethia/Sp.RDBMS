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
	private boolean unique;
	private long id;
	private long parentId;
	private PhysicalAddress rootAddress;
	private PhysicalAddress storedAddress;
	private long pageCount;
	private long recordsCount;
	private int recordSize;
	private int keySize;
	private long creationDate;
	private long lastModified;
	private Vector<Attribute> attributes;

	private DynamicObject dObject;
	public BPlusTree bTree;

	public Index(String _indexName, long _id, long _parentId, boolean _unique, Vector<Attribute> _attributes) {
		indexName = _indexName;
		fileName = _indexName + _id + ".index";
		id = _id;
		parentId = _parentId;
		unique = _unique;
		creationDate = (new Date()).getTime();
		lastModified = (new Date()).getTime();
		pageCount = 1;
		recordsCount = 0;
		attributes = _attributes;
		keySize = 0;
		for (int i = 0; i < attributes.size(); i++) {
			keySize += attributes.get(i).getAttributeSize();
		}
		nKeys = 64;
		while (nKeys > 0) {
			if ((DiskSpaceManager.PAGE_SIZE * 8 - 7) / (1 + 8 * (nKeys * (20 + keySize) + 28)) >= 1) {
				break;
			} else {
				nKeys--;
			}
		}
		recordSize = nKeys * (keySize + 20) + 28;
		rootAddress = new PhysicalAddress();
		storedAddress = new PhysicalAddress();
	}

	public Index(ByteBuffer serializedBuffer) {
		indexName = "";
		for (int i = 0; i < INDEX_NAME_LENGTH; i++) {
			if (serializedBuffer.getChar(2 * i) != '\0') {
				indexName += serializedBuffer.getChar(2 * i);
			}
		}
		rootAddress = new PhysicalAddress();
		storedAddress = new PhysicalAddress();
		serializedBuffer.position(INDEX_NAME_LENGTH * 2);
		id = serializedBuffer.getLong();
		parentId = serializedBuffer.getLong();
		nKeys = serializedBuffer.getInt();
		keySize = serializedBuffer.getInt();
		recordSize = serializedBuffer.getInt();
		rootAddress.id = serializedBuffer.getLong();
		rootAddress.pageNumber = serializedBuffer.getLong();
		rootAddress.pageOffset = serializedBuffer.getInt();
		storedAddress.id = serializedBuffer.getLong();
		storedAddress.pageNumber = serializedBuffer.getLong();
		storedAddress.pageOffset = serializedBuffer.getInt();
		pageCount = serializedBuffer.getLong();
		recordsCount = serializedBuffer.getLong();
		creationDate = serializedBuffer.getLong();
		lastModified = serializedBuffer.getLong();
		unique = serializedBuffer.getInt() == 1;
		fileName = indexName + id + ".index";
		attributes = new Vector<Attribute>();
	}

	public void addAttribute(Attribute attr, boolean addToSize) {
		attributes.add(attr);
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

	public long getId() {
		return id;
	}

	public String getName() {
		return indexName;
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

	public int getNumberOfKeys() {
		return nKeys;
	}

	public int getRecordSize() {
		return recordSize;
	}

	public ByteBuffer serialize() {
		ByteBuffer serializedBuffer = ByteBuffer.allocate((int) SystemCatalogManager.INDEX_RECORD_SIZE);
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
		serializedBuffer.putLong(rootAddress.id);
		serializedBuffer.putLong(rootAddress.pageNumber);
		serializedBuffer.putInt(rootAddress.pageOffset);
		serializedBuffer.putLong(storedAddress.id);
		serializedBuffer.putLong(storedAddress.pageNumber);
		serializedBuffer.putInt(storedAddress.pageOffset);
		serializedBuffer.putLong(pageCount);
		serializedBuffer.putLong(recordsCount);
		serializedBuffer.putLong(creationDate);
		serializedBuffer.putLong(lastModified);
		serializedBuffer.putInt(unique ? 1 : 0);
		return serializedBuffer;
	}

	public void setName(String _indexName) {
		indexName = _indexName;
		lastModified = (new Date()).getTime();
	}

	public void setRecordSize(int _recordSize) {
		recordSize = _recordSize;
	}

	public void setRoot(PhysicalAddress _physicalAddress) {
		rootAddress = _physicalAddress;
	}

	public boolean containsDuplicates() {
		return !unique;
	}

	public void setAddress(long _id, long _pageNumber, int _recordOffset) {
		storedAddress = new PhysicalAddress(_id,_pageNumber,_recordOffset);
	}

	public PhysicalAddress getRootAddress() {
		return rootAddress;
	}

	public PhysicalAddress getAddress() {
		return storedAddress;
	}
	
	public long getPageNumber() {
		return storedAddress.pageNumber;
	}

	public int getRecordOffset() {
		return storedAddress.pageOffset;
	}

	public int getKeySize() {
		return keySize;
	}

	public Vector<Attribute> getAttributes() {
		return attributes;
	}

	public long getParentId() {
		return parentId;
	}

	public boolean setTree() {
		if (dObject == null) {
			dObject = new DynamicObject(attributes);
			bTree = new BPlusTree(this, dObject);
			return true;
		}
		return false;
	}

	public boolean insert(DynamicObject object, PhysicalAddress value) {
		if (dObject != null) {
			return bTree.insert(object, value);
		}
		return false;
	}
	
	public boolean delete(DynamicObject object, PhysicalAddress value) {
		if (dObject != null) {
			return bTree.delete(object, value);
		}
		return false;
	}

	public PhysicalAddress search(DynamicObject object) {
		if (dObject != null) {
			setTree();
			return bTree.search(object);
		}
		return null;
	}
}
