package databaseManager;

import java.nio.ByteBuffer;

public class BPlusTree {

	/**
	 * the maximum number of keys in inner node, the number of pointer is N+1, N
	 * must be > 2
	 */

	private int M;
	private int N;
	private int recordSize;
	private Index index;
	private Node rootNode;
	private Node searchNode;
	private BufferManager bufferManager;
	private PhysicalAddress rootAddress;
	private PhysicalAddress prevAddress;
	private DynamicObject tempObject;
	private int currentLocation;
	private int currentBucketPtr;
	private int resultLocation;
	private int resultBucketPtr;
	private int bucketNumber;

	public BPlusTree(Index _index, DynamicObject _tempObject) {
		index = _index;
		recordSize = index.getRecordSize();
		M = index.getNumberOfKeys();
		N = recordSize / 20 - 1;
		recordSize = index.getRecordSize();
		tempObject = _tempObject;
		bufferManager = BufferManager.getBufferManager();
		openIndex();
	}

	public void openIndex() {
		if (index.getRootAddress() == null || index.getRootAddress().id == -1) {
			rootNode = new Node();
			rootNode.isLeaf = true;
			updateIndexHead(rootNode);
		} else {
			readHead();
		}
	}

	public void closeIndex() {
		bufferManager.closeFile(index.getId());
	}

	public boolean delete(DynamicObject key, PhysicalAddress value) {
		PhysicalAddress result = search(key);
		if (result != null && result.pageOffset > -1) {
			if (index.containsDuplicates()) {
				boolean flag=false;
				while (result != null && result.pageOffset > -1) {
					if (result.equals(value)) {
						flag = true;
						break;
					}
					result = getFromBucket(key);
				}
				if(flag){
					ByteBuffer serialData = bufferManager.read(prevAddress.id, prevAddress.pageNumber);
					Bucket bucket = new Bucket(serialData, prevAddress.pageOffset);
					bucket.pointers[resultBucketPtr] = new PhysicalAddress(-2, -2,-2);
					bufferManager.write(index.getId(), prevAddress.pageNumber, prevAddress.pageOffset, bucket.serialize());
					return true;
				}
			} else {
				searchNode.childrens[resultLocation] = new PhysicalAddress(-2, -2,-2);
				bufferManager.write(index.getId(), prevAddress.pageNumber, prevAddress.pageOffset, searchNode.serialize());
				return true;
			}
		}
		return false;
	}

	public PhysicalAddress search(DynamicObject key) {
		ByteBuffer serialData = bufferManager.read(rootAddress.id, rootAddress.pageNumber);
		Node node = new Node(serialData, rootAddress.pageOffset);
		prevAddress = rootAddress;
		while (true) {
			currentLocation = node.getLocation(key);
			if (node.isLeaf) {
				if (!node.keys[currentLocation-1].equals(key)) {
					currentLocation = 0;
				}
				break;
			}
			prevAddress = node.childrens[currentLocation];
			ByteBuffer serializedBuffer = bufferManager.read(node.childrens[currentLocation].id, node.childrens[currentLocation].pageNumber);
			node = new Node(serializedBuffer, node.childrens[currentLocation].pageOffset);
		}
		if (currentLocation == 0) {
			return null;
		}
		currentBucketPtr = 0;
		searchNode = node;
		return getFromBucket(key);
	}

	public PhysicalAddress getFromBucket(DynamicObject key) {
		PhysicalAddress returnData = new PhysicalAddress();
		returnData = searchNode.childrens[currentLocation];
		PhysicalAddress nextEntry = new PhysicalAddress();
		if (index.containsDuplicates()) {
			Bucket bucket = new Bucket();
			PhysicalAddress nextAddress = searchNode.childrens[currentLocation];
			for (int i = 0; i <= bucketNumber; i++) {
				if (nextAddress.id < 0 && nextAddress.pageNumber < 0) {
					return null;
				}
				prevAddress = nextAddress;
				ByteBuffer serialData = bufferManager.read(nextAddress.id, nextAddress.pageNumber);
				bucket = new Bucket(serialData, nextAddress.pageOffset);
				nextAddress = bucket.nextBucket;
			}
			boolean flag = false;
			while (!flag) {
				if (bucket.pointers.length == currentBucketPtr) {
					bucketNumber++;
					if (nextAddress.id < 0 && nextAddress.pageNumber < 0) {
						return null;
					}
					prevAddress = nextAddress;
					ByteBuffer serialData = bufferManager.read(nextAddress.id, nextAddress.pageNumber);
					bucket = new Bucket(serialData, nextAddress.pageOffset);
					nextAddress = bucket.nextBucket;
					currentBucketPtr = 0;
				}
				while (bucket.pointers.length > currentBucketPtr) {
					returnData = bucket.pointers[currentBucketPtr++];
					nextEntry = bucket.pointers[currentBucketPtr];
					if (returnData.id >= 0) {
						flag = true;
						break;
					}
				}
			}
		}
		resultLocation = currentLocation;
		resultBucketPtr = currentBucketPtr-1;
		if (!index.containsDuplicates() || nextEntry.id == -1) {
			bucketNumber = 0;
			currentBucketPtr = 0;
			currentLocation++;
			if (currentLocation > searchNode.num) {
				currentLocation = 0;
			}
		}
		return returnData;
	}

	public boolean insert(DynamicObject key, PhysicalAddress value) {
		Split result = insert(rootAddress, key, value);

		if (result.error == 1) {
			return false;
		}
		if (result.hasSplit) {
			// The old root was splitted in two parts.
			// We have to create a new root pointing to them
			Node _root = new Node();
			_root.isLeaf = false;
			_root.num = 1;
			_root.keys[0] = result.key;
			_root.childrens[1] = result.value;
			_root.childrens[0] = rootAddress;
			updateIndexHead(_root);
		}
		return true;
	}

	private void updateIndexHead(Node _root) {
		long freePageNumber = bufferManager.getFreePageNumber(index.getId());
		int rootOffset = bufferManager.getFreeRecordOffset(index.getId(), freePageNumber, index.getRecordsPerPage(), index.getRecordSize());
		rootAddress = new PhysicalAddress(index.getId(), freePageNumber,rootOffset);
		bufferManager.write(index.getId(), freePageNumber, rootOffset, _root.serialize());
		int recordNumber = (rootOffset - (index.getRecordsPerPage() + 7) / 8) / index.getRecordSize();
		bufferManager.writeRecordBitmap(index.getId(), freePageNumber, index.getRecordsPerPage(), recordNumber, true);
		index.setRoot(rootAddress);
		DatabaseManager.getSystemCatalog().updateIndexCatalog(index);
		rootNode = _root;
	}

	private void readHead() {
		rootAddress = index.getRootAddress();
		ByteBuffer serialData = bufferManager.read(rootAddress.id, rootAddress.pageNumber);
		rootNode = new Node(serialData, rootAddress.pageOffset);
	}

	private Split insertKey(Node node, int position, DynamicObject key, PhysicalAddress value) {
		Split split = new Split();
		Node tempNode = new Node();
		int midKey = (node.num + 1) < M ? (node.num + 1) : M / 2;
		int remaining = node.num + 1 - midKey;
		node.serialize();
		for (int i = M / 2; i < position; i++) {
			tempNode.keys[i] = node.keys[i];
			tempNode.childrens[i + 1] = node.childrens[i + 1];
		}

		tempNode.keys[position] = key;
		tempNode.childrens[position + 1] = value;
		for (int i = position; i < node.num; i++) {
			tempNode.keys[i + 1] = node.keys[i];
			tempNode.childrens[i + 2] = node.childrens[i + 1];
		}
		node.serialize();
		for (int i = position; i < midKey; i++) {
			node.keys[i] = tempNode.keys[i];
			node.childrens[i + 1] = tempNode.childrens[i + 1];
		}
		node.num = midKey;
		node.serialize();
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
				copyPosition++;
			}

			newNode.childrens[remaining] = tempNode.childrens[copyPosition];

			newNode.num = remaining;
			long freePageNumber = bufferManager.getFreePageNumber(index.getId());
			int offset = bufferManager.getFreeRecordOffset(index.getId(), freePageNumber, index.getRecordsPerPage(), index.getRecordSize());

			key = tempNode.keys[M / 2];
			value = new PhysicalAddress(index.getId(), freePageNumber,offset);

			if (newNode.isLeaf) {
				newNode.childrens[0] = node.childrens[0];

				node.childrens[0] = new PhysicalAddress(index.getId(), freePageNumber,offset);
			}

			bufferManager.write(index.getId(), freePageNumber, offset, newNode.serialize());
			int recordNumber = (offset - (index.getRecordsPerPage() + 7) / 8) / index.getRecordSize();
			bufferManager.writeRecordBitmap(index.getId(), freePageNumber, index.getRecordsPerPage(), recordNumber, true);
		}
		split.hasSplit = remaining > 0;
		split.key = key;
		split.value = value;

		return split;
	}

	private Split insert(PhysicalAddress nodeAddress, DynamicObject key, PhysicalAddress value) {
		ByteBuffer serialData = bufferManager.read(nodeAddress.id, nodeAddress.pageNumber);
		Node node = new Node(serialData, nodeAddress.pageOffset);
		node.serialize();
		int i = node.getLocation(key);
		boolean areEqual = (i > 0) && key.equals(node.keys[i - 1]);
		Split split = new Split();
		boolean isSplit = false;
		if (!node.isLeaf) {
			Split tempSplit = insert(node.childrens[i], key, value);
			isSplit = tempSplit.hasSplit;
			key = tempSplit.key;
			value = tempSplit.value;
		}

		if (isSplit || node.isLeaf && !areEqual) {
			if (node.isLeaf && index.containsDuplicates()) {
				Bucket _bucket = new Bucket();
				insertToBucket(_bucket, value);
				long freePageNumber = bufferManager.getFreePageNumber(index.getId());
				int offset = bufferManager.getFreeRecordOffset(index.getId(), freePageNumber, index.getRecordsPerPage(), index.getRecordSize());
				bufferManager.write(index.getId(), freePageNumber, offset, _bucket.serialize());
				int recordNumber = (offset - (index.getRecordsPerPage() + 7) / 8) / index.getRecordSize();
				bufferManager.writeRecordBitmap(index.getId(), freePageNumber, index.getRecordsPerPage(), recordNumber, true);
				value.id = index.getId();
				value.pageNumber = freePageNumber;
				value.pageOffset = offset;
			}
			node.serialize();
			Split tempSplit = insertKey(node, i, key, value);
			isSplit = tempSplit.hasSplit;
			key = tempSplit.key;
			value = tempSplit.value;
			bufferManager.write(index.getId(), nodeAddress.pageNumber, nodeAddress.pageOffset, node.serialize());
			int recordNumber = (nodeAddress.pageOffset - (index.getRecordsPerPage() + 7) / 8) / index.getRecordSize();
			bufferManager.writeRecordBitmap(index.getId(), nodeAddress.pageNumber, index.getRecordsPerPage(), recordNumber, true);
		} else if (node.isLeaf && index.containsDuplicates()) {
			ByteBuffer serialBuffer = bufferManager.read(node.childrens[i].id, node.childrens[i].pageNumber);
			Bucket bucket = new Bucket(serialBuffer, node.childrens[i].pageOffset);
			insertToBucket(bucket, value);
			bufferManager.write(node.childrens[i].id, node.childrens[i].pageNumber, node.childrens[i].pageOffset, bucket.serialize());
			int recordNumber = (node.childrens[i].pageOffset - (index.getRecordsPerPage() + 7) / 8) / index.getRecordSize();
			bufferManager.writeRecordBitmap(index.getId(), node.childrens[i].pageNumber, index.getRecordsPerPage(), recordNumber, true);
		} else {
			split.error = 1;
		}

		split.hasSplit = isSplit;
		split.key = key;
		split.value = value;
		return split;
	}

	private void insertToBucket(Bucket bucket, PhysicalAddress value) {
		int i;
		boolean flag = false;
		for (i = 0; i < N; i++) {
			if (bucket.pointers[i].pageOffset < 0) {
				bucket.pointers[i] = value;
				i++;
				flag = true;
				break;
			}
		}
		if (flag) {
			return;
		}
		Bucket _bucket = new Bucket();
		insertToBucket(_bucket, value);
		long freePageNumber = bufferManager.getFreePageNumber(index.getId());
		int offset = bufferManager.getFreeRecordOffset(index.getId(), freePageNumber, index.getRecordsPerPage(), index.getRecordSize());
		bufferManager.write(index.getId(), freePageNumber, offset, _bucket.serialize());
		int recordNumber = (offset - (index.getRecordsPerPage() + 7) / 8) / index.getRecordSize();
		bufferManager.writeRecordBitmap(index.getId(), freePageNumber, index.getRecordsPerPage(), recordNumber, true);
		bucket.nextBucket = new PhysicalAddress(index.getId(), freePageNumber,offset);
	}

	class Node {
		boolean isLeaf;
		int num;
		DynamicObject[] keys;
		PhysicalAddress[] childrens;

		public Node() {
			keys = new DynamicObject[M];
			childrens = new PhysicalAddress[M + 1];
			for (int i = 0; i < M; i++) {
				keys[i] = new DynamicObject(tempObject.attributes);
				childrens[i] = new PhysicalAddress(-1, -1,-1);
			}
		}

		public Node(ByteBuffer serialData, int pos) {

			keys = new DynamicObject[M];
			childrens = new PhysicalAddress[M + 1];

			serialData.position(pos);
			isLeaf = serialData.getInt() == 1;
			num = serialData.getInt();
			for (int i = 0; i < M; i++) {
				byte[] serialBytes = new byte[index.getKeySize()];
				serialData.get(serialBytes);
				keys[i] = tempObject.deserialize(serialBytes);
			}
			for (int i = 0; i < M; i++) {
				childrens[i] = new PhysicalAddress();
				childrens[i].id = serialData.getLong();
				childrens[i].pageNumber = serialData.getLong();
				childrens[i].pageOffset = serialData.getInt();
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
				serialData.putLong(childrens[i].pageNumber);
				serialData.putInt(childrens[i].pageOffset);
			}
			return serialData;
		}

		/**
		 * Returns the position where 'key' should be inserted in a leaf node
		 * that has the given keys.
		 */
		private int getLocation(DynamicObject key) {
			for (int i = 0; i < num; i++) {
				if (keys[i].compareTo(key) > 0) {
					if(i>0){
						if(childrens[i-1].id!=-2){
							return i;
						}
					}else{
						return i;
					}
				}
			}
			return num;
		}

	}

	class Bucket {
		public PhysicalAddress[] pointers;

		public PhysicalAddress nextBucket;
		public Bucket() {
			pointers = new PhysicalAddress[N];
			for (int i = 0; i < N; i++) {
				pointers[i] = new PhysicalAddress(-1, -1,-1);
			}
			nextBucket = new PhysicalAddress(-1, -1,-1);
		}

		public Bucket(ByteBuffer serialData, int position) {
			serialData.position(position);
			pointers = new PhysicalAddress[N];
			for (int i = 0; i < N; i++) {
				pointers[i] = new PhysicalAddress(serialData.getLong(), serialData.getLong(),serialData.getInt());
			}
			nextBucket = new PhysicalAddress(serialData.getLong(), serialData.getLong(),serialData.getInt());
		}

		public ByteBuffer serialize() {
			ByteBuffer serialData = ByteBuffer.allocate(recordSize);
			for (int i = 0; i < N; i++) {
				serialData.putLong(pointers[i].id);
				serialData.putLong(pointers[i].pageNumber);
				serialData.putInt(pointers[i].pageOffset);
			}
			serialData.putLong(nextBucket.id);
			serialData.putLong(nextBucket.pageNumber);
			serialData.putInt(nextBucket.pageOffset);
			return serialData;
		}
	}

	public class Split {
		public DynamicObject key;
		public PhysicalAddress value;
		public boolean hasSplit;
		public int error;

		public Split() {
			value = new PhysicalAddress();
			hasSplit = false;
			error = 0;
		}

		public Split(DynamicObject k, PhysicalAddress l) {
			key = k;
			value = l;
			hasSplit = false;
			error = 0;
		}
	}
}
