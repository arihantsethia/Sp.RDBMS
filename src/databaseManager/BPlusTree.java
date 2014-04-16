package databaseManager;

import java.nio.ByteBuffer;

public class BPlusTree{

    /**
     * the maximum number of keys in inner node, the number of pointer is N+1, N
     * must be > 2
     */

    private static int M;
    private static int N;
    private int recordSize;
    private Index index;
    private Node rootNode;
    private BufferManager bufferManager;
    private PhysicalAddress rootAddress;
    private int rootOffset;
    private DynamicObject tempObject;

    public BPlusTree(Index _index, DynamicObject _tempObject) {
	index = _index;
	M = index.getNumberOfKeys();
	N = recordSize / 20 - 1;
	recordSize = index.getRecordSize();
	tempObject = _tempObject;
	bufferManager = BufferManager.getBufferManager();
	openIndex();
    }

    public void openIndex() {
	if (index.getFileSize() == 0) {
	    rootNode = new Node();
	    rootNode.isLeaf = true;
	    updateIndexHead(rootNode);
	} else {
	    readHead();
	}
    }

    public void closeIndex() {
	bufferManager.openFile(index.getIndexId());
    }

    public PhysicalAddress search(DynamicObject key) {
	Node node = rootNode;
	while (true) {
	    if (node.isLeaf) {
		break;
	    }
	    int pos = node.getLocation(key);
	    ByteBuffer serializedBuffer = bufferManager.read(node.childrens[pos].id, node.childrens[pos].offset);
	    node = new Node(serializedBuffer, node.offset[pos]);
	}
	return getFromBucket(node, key);
    }

    private PhysicalAddress getFromBucket(Node node, DynamicObject key) {
	// TODO Auto-generated method stub
	return null;
    }

    public void insert(DynamicObject key, PhysicalAddress value, int recordOffset) {
	Split result = insert(rootNode, key, value, recordOffset);
	if (result.hasSplit) {
	    // The old root was splitted in two parts.
	    // We have to create a new root pointing to them
	    Node _root = new Node();
	    _root.isLeaf = true;
	    _root.num = 1;
	    _root.keys[0] = result.key;
	    _root.childrens[1] = result.value;
	    _root.childrens[1] = new PhysicalAddress(index.getIndexId(), result.recordOffset);
	    _root.childrens[0] = rootAddress;
	    updateIndexHead(_root);
	}
    }

    private void updateIndexHead() {
	bufferManager.write(index.getIndexId(), rootAddress.offset, rootOffset, rootNode.serialize());
    }

    private void updateIndexHead(Node _root) {
	long freePageNumber = bufferManager.getFreePageNumber(index.getIndexId());
	rootOffset = bufferManager.getFreeRecordOffset(index.getIndexId(), freePageNumber, index.getRecordsPerPage(), index.getRecordSize());
	rootAddress = new PhysicalAddress(index.getIndexId(), freePageNumber);
	bufferManager.write(index.getIndexId(), freePageNumber, rootOffset, _root.serialize());
	int recordNumber = (rootOffset - (index.getRecordsPerPage() + 7) / 8) / index.getRecordSize();
	bufferManager.writeRecordBitmap(index.getIndexId(), freePageNumber, index.getRecordsPerPage(), recordNumber, true);
	index.setRoot(rootAddress, rootOffset);
	rootNode = _root;
    }

    private void readHead() {
	rootAddress = index.getRootPageAddress();
	rootOffset = index.getRootOffset();
	ByteBuffer serialData = bufferManager.read(rootAddress.id, rootAddress.offset);
	rootNode = new Node(serialData, rootOffset);
    }

    private Split insertKey(Node node, int position, DynamicObject key, PhysicalAddress value, int recordOffset) {
	Split split = new Split();
	Node tempNode = new Node();
	int midKey = (node.num + 1) < M ? (node.num + 1) : M / 2;
	for (int i = M / 2; i < position; i++) {
	    tempNode.keys[i] = node.keys[i];
	    tempNode.childrens[i + 1] = node.childrens[i];
	    tempNode.offset[i + 1] = node.offset[i];
	}

	tempNode.keys[position] = key;
	tempNode.childrens[position + 1] = value;
	tempNode.offset[position + 1] = recordOffset;

	for (int i = position; i < node.num; i++) {
	    tempNode.keys[i + 1] = node.keys[i];
	    tempNode.childrens[i + 2] = node.childrens[i];
	    tempNode.offset[i + 2] = node.offset[i];
	}

	for (int i = position; i < midKey; i++) {
	    node.keys[i] = tempNode.keys[i];
	    node.childrens[i + 1] = tempNode.childrens[i + 1];
	    node.offset[i + 1] = tempNode.offset[i + 1];
	}
	node.num = midKey;

	int remaining = node.num - midKey;

	if (remaining > 0) {
	    Node newNode = new Node();
	    newNode.isLeaf = node.isLeaf;
	    int copyPosition = M / 2;
	    if (!newNode.isLeaf) {
		remaining = remaining - 1;
		copyPosition = copyPosition + 1;
	    }

	    for (int i = 0; i < remaining; i++) {
		newNode.keys[i] = tempNode.keys[copyPosition];
		newNode.childrens[i] = tempNode.childrens[copyPosition];
		newNode.offset[i] = tempNode.offset[copyPosition];
		copyPosition++;
	    }

	    newNode.childrens[remaining] = tempNode.childrens[copyPosition];
	    newNode.offset[remaining] = tempNode.offset[copyPosition];

	    newNode.num = remaining;

	    long freePageNumber = bufferManager.getFreePageNumber(index.getIndexId());
	    int offset = bufferManager.getFreeRecordOffset(index.getIndexId(), freePageNumber, index.getRecordsPerPage(), index.getRecordSize());

	    if (newNode.isLeaf) {
		newNode.childrens[0] = node.childrens[0];
		newNode.offset[0] = node.offset[0];

		node.childrens[0] = new PhysicalAddress(index.getIndexId(), freePageNumber);
		node.offset[0] = offset;
	    }

	    bufferManager.write(index.getIndexId(), freePageNumber, offset, newNode.serialize());
	    int recordNumber = (offset - (index.getRecordsPerPage() + 7) / 8) / index.getRecordSize();
	    bufferManager.writeRecordBitmap(index.getIndexId(), freePageNumber, index.getRecordsPerPage(), recordNumber, true);
	    updateIndexHead();
	}
	split.hasSplit = remaining>0;
	split.key = key;
	split.value = value;
	split.recordOffset = recordOffset;

	return split;
    }

    private Split insert(Node node, DynamicObject key, PhysicalAddress value, int recordOffset) {
	int i = node.getLocation(key);
	boolean areEqual = key.equals(node.keys[i]);
	Split split = new Split();
	boolean isSplit = false;
	if (!node.isLeaf) {
	    ByteBuffer serialData = bufferManager.read(node.childrens[i].id, node.childrens[i].offset);
	    Node tempNode = new Node(serialData, node.offset[i]);
	    Split tempSplit = insert(tempNode, key, value, recordOffset);
	    isSplit = tempSplit.hasSplit;
	    key = tempSplit.key;
	    value = tempSplit.value;
	    recordOffset = tempSplit.recordOffset;
	}

	if ((isSplit || node.isLeaf) && !areEqual) {
	    if (node.isLeaf && index.containsDuplicates()) {
		Bucket _bucket = new Bucket();
		insertToBucket(_bucket, value, recordOffset);
		long freePageNumber = bufferManager.getFreePageNumber(index.getIndexId());
		int offset = bufferManager.getFreeRecordOffset(index.getIndexId(), freePageNumber, index.getRecordsPerPage(), index.getRecordSize());
		bufferManager.write(index.getIndexId(), freePageNumber, offset, _bucket.serialize());
		int recordNumber = (offset - (index.getRecordsPerPage() + 7) / 8) / index.getRecordSize();
		bufferManager.writeRecordBitmap(index.getIndexId(), freePageNumber, index.getRecordsPerPage(), recordNumber, true);
		value.id = index.getIndexId();
		value.offset = freePageNumber;
		recordOffset = offset;
		updateIndexHead();
	    }
	    Split tempSplit = insertKey(node, i, key, value, recordOffset);
	    isSplit = tempSplit.hasSplit;
	    key = tempSplit.key;
	    value = tempSplit.value;

	    long freePageNumber = bufferManager.getFreePageNumber(index.getIndexId());
	    int offset = bufferManager.getFreeRecordOffset(index.getIndexId(), freePageNumber, index.getRecordsPerPage(), index.getRecordSize());
	    bufferManager.write(index.getIndexId(), freePageNumber, offset, node.serialize());
	    int recordNumber = (offset - (index.getRecordsPerPage() + 7) / 8) / index.getRecordSize();
	    bufferManager.writeRecordBitmap(index.getIndexId(), freePageNumber, index.getRecordsPerPage(), recordNumber, true);

	} else if (node.isLeaf && index.containsDuplicates()) {
	    ByteBuffer serialData = bufferManager.read(node.childrens[i].id, node.childrens[i].offset);
	    Bucket bucket = new Bucket(serialData, node.offset[i]);
	    insertToBucket(bucket, value, recordOffset);
	    long freePageNumber = bufferManager.getFreePageNumber(index.getIndexId());
	    int offset = bufferManager.getFreeRecordOffset(index.getIndexId(), freePageNumber, index.getRecordsPerPage(), index.getRecordSize());
	    bufferManager.write(index.getIndexId(), freePageNumber, offset, bucket.serialize());
	    int recordNumber = (offset - (index.getRecordsPerPage() + 7) / 8) / index.getRecordSize();
	    bufferManager.writeRecordBitmap(index.getIndexId(), freePageNumber, index.getRecordsPerPage(), recordNumber, true);
	} else {
	    split.error = 1;
	}

	split.hasSplit = isSplit;
	split.key = key;
	split.value = value;
	split.recordOffset = recordOffset;
	return split;
    }

    private void insertToBucket(Bucket bucket, PhysicalAddress value, int recordOffset) {
	for (int i = 0; i < N; i++) {
	    if (bucket.offset[i] == -1) {
		bucket.pointers[i] = value;
		bucket.offset[i] = recordOffset;
		return;
	    }
	}
	Bucket _bucket = new Bucket();
	insertToBucket(_bucket, value, recordOffset);
	long freePageNumber = bufferManager.getFreePageNumber(index.getIndexId());
	int offset = bufferManager.getFreeRecordOffset(index.getIndexId(), freePageNumber, index.getRecordsPerPage(), index.getRecordSize());
	bufferManager.write(index.getIndexId(), freePageNumber, offset, _bucket.serialize());
	int recordNumber = (offset - (index.getRecordsPerPage() + 7) / 8) / index.getRecordSize();
	bufferManager.writeRecordBitmap(index.getIndexId(), freePageNumber, index.getRecordsPerPage(), recordNumber, true);
	bucket.nextBucket = new PhysicalAddress(index.getIndexId(), freePageNumber);
	bucket.nextBucketOffset = offset;
    }

    class Node {
	boolean isLeaf;
	int num;
	DynamicObject[] keys;
	PhysicalAddress[] childrens;
	int[] offset;

	public Node() {
	    keys = new DynamicObject[M];
	    childrens = new PhysicalAddress[M + 1];
	    offset = new int[M + 1];
	    for(int i=0; i <M;i++){
		keys[i] = new DynamicObject(index.getNumberOfKeys());
		childrens[i] = new PhysicalAddress(-1,-1);
		offset[i] = -1;
	    }
	}

	public Node(ByteBuffer serialData, int pos) {
	    serialData.position(pos);
	    isLeaf = serialData.getInt() == 1;
	    num = serialData.getInt();
	    for (int i = 0; i < M; i++) {
		byte[] serialBytes = new byte[index.getKeySize()];
		serialData.get(serialBytes);
		keys[i] = tempObject.deserialize(serialBytes);
	    }
	    for (int i = 0; i < M; i++) {
		childrens[i].id = serialData.getLong();
		childrens[i].offset = serialData.getLong();
	    }
	    for (int i = 0; i < M; i++) {
		offset[i] = serialData.getInt();
	    }
	}

	public ByteBuffer serialize() {
	    ByteBuffer serialData = ByteBuffer.allocate(recordSize);
	    serialData.putInt(isLeaf ? 1 : 0);
	    serialData.putInt(num);
	    for (int i = 0; i < M; i++) {
		serialData.put(tempObject.serialize(keys[i]));
	    }
	    for (int i = 0; i < M; i++) {
		serialData.putLong(childrens[i].id);
		serialData.putLong(childrens[i].offset);
	    }
	    for (int i = 0; i < M; i++) {
		serialData.putInt(offset[i]);
	    }
	    return serialData;
	}

	/**
	 * Returns the position where 'key' should be inserted in a leaf node
	 * that has the given keys.
	 */
	private int getLocation(DynamicObject key) {
	    for (int i = 0; i < num; i++) {
		if (keys[i].compareTo(key) >= 0) {
		    return i;
		}
	    }
	    return num;
	}

    }

    class Bucket {
	public PhysicalAddress[] pointers;
	public int[] offset;

	public PhysicalAddress nextBucket;
	public int nextBucketOffset;

	public Bucket() {
	    pointers = new PhysicalAddress[N];
	    offset = new int[N];
	    for (int i = 0; i < N; i++) {
		pointers[i] = new PhysicalAddress(-1, -1);
		offset[i] = -1;
	    }
	    nextBucket = new PhysicalAddress(-1, -1);
	    nextBucketOffset = -1;
	}

	public Bucket(ByteBuffer serialData, int position) {
	    serialData.position(position);
	    for (int i = 0; i < M; i++) {
		pointers[i].id = serialData.getLong();
		pointers[i].offset = serialData.getLong();
	    }
	    for (int i = 0; i < M; i++) {
		offset[i] = serialData.getInt();
	    }
	    nextBucket.id = serialData.getLong();
	    nextBucket.offset = serialData.getLong();
	    nextBucketOffset = serialData.getInt();	    
	}

	public ByteBuffer serialize() {
	    ByteBuffer serialData = ByteBuffer.allocate(recordSize);
	    for (int i = 0; i < M; i++) {
		serialData.putLong(pointers[i].id);
		serialData.putLong(pointers[i].offset);
	    }
	    for (int i = 0; i < M; i++) {
		serialData.putInt(offset[i]);
	    }
	    serialData.putLong(nextBucket.id);
	    serialData.putLong(nextBucket.offset);
	    serialData.putInt(nextBucketOffset);
	    return serialData;
	}
    }

    class Split {
	public DynamicObject key;
	public PhysicalAddress value;
	public int recordOffset;
	public boolean hasSplit;
	public int error;
	
	public Split() {
	    recordOffset = -1;
	    hasSplit = false;
	    error = 0;
	}
	

	public Split(DynamicObject k, PhysicalAddress l, int lOffset) {
	    key = k;
	    value = l;
	    recordOffset = lOffset;
	    hasSplit = false;
	    error = 0;
	}
    }
}
