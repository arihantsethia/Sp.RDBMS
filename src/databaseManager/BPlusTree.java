package databaseManager;

import java.nio.ByteBuffer;

public class BPlusTree<Key extends Comparable<? super Key>> {

	/**
	 * the maximum number of keys in inner node, the number of pointer is N+1, N
	 * must be > 2
	 */

	private static final int M = 4;

	private Index index;
	private Node rootNode;
	private BufferManager bufferManager;
	private PhysicalAddress rootAddress;
	private int rootOffset;

	public BPlusTree() {

	}

	public void openIndex(Index _index) {
		index = _index;
		if (index.getFileSize() == 0) {
			rootNode = new Node();
			updateIndexHead(rootNode);
		} else {
			readHead();
		}
	}

	public void closeIndex() {
		bufferManager.openFile(index.getIndexId());
	}

	public void insert(Key key, PhysicalAddress value, int recordOffset) {
		Split result = rootNode.insert(key, value,recordOffset);
		if (result != null) {
			// The old root was splitted in two parts.
			// We have to create a new root pointing to them
			Node _root = new Node();
		 	_root.num = 1;
		 	_root.keys[0] = result.key;
		 	_root.childrens[0] = result.left;
		 	_root.childrens[1] = result.right;
			updateIndexHead(_root);
		}
	}

	private void updateIndexHead() {
		bufferManager.write(index.getIndexId(), rootAddress.offset, rootOffset,
				rootNode.serialize());
	}

	private void updateIndexHead(Node _root) {
		long freePageNumber = bufferManager.getFreePageNumber(index
				.getIndexId());
		rootOffset = bufferManager.getFreeRecordOffset(index.getIndexId(),
				freePageNumber, index.getRecordsPerPage(),
				index.getRecordSize());
		rootAddress = new PhysicalAddress(index.getIndexId(), freePageNumber);
		bufferManager.write(index.getIndexId(), freePageNumber, rootOffset,
				_root.serialize());
		int recordNumber = (rootOffset - (index.getRecordsPerPage() + 7) / 8)
				/ index.getRecordSize();
		bufferManager.writeRecordBitmap(index.getIndexId(), freePageNumber,
				index.getRecordsPerPage(), recordNumber, true);
		index.setRoot(rootAddress, rootOffset);
		rootNode = _root;
	}

	private void readHead() {

	}

	class Node {
		boolean isLeaf;
		int num;
		Key[] keys;
		PhysicalAddress[] childrens;
		int[] offset;

		public Node() {
			keys = (Key[]) new Object[M];
			childrens = new PhysicalAddress[M+1];
			offset = new int[M+1];
		}

		public Node(ByteBuffer serialData, int i) {
			// TODO Auto-generated constructor stub
		}

		public ByteBuffer serialize() {
			return null;
		}

		/**
		 * Returns the position where 'key' should be inserted in a leaf node
		 * that has the given keys.
		 */
		private int getLocation(Key key) {
			for (int i = 0; i < num; i++) {
				if (keys[i].compareTo(key) >= 0) {
					return i;
				}
			}
			return num;
		}

		public Split insert(Key key, PhysicalAddress value, int recordOffset) {
			int i = getLocation(key);
			boolean areEqual = key.equals(keys[i]);
			Split split = null;
			if (!isLeaf) {
				ByteBuffer serialData = bufferManager.read(childrens[i].id,
						childrens[i].offset);
				Node tempNode = new Node(serialData, offset[i]);
				split = tempNode.insert(key, value, recordOffset);
			}

			if ((split != null || isLeaf) && !areEqual) {
				//Kuch toh karna hai idhar :P
			} else if (isLeaf && index.containsDuplicates()) {
				ByteBuffer serialData = bufferManager.read(childrens[i].id,
						childrens[i].offset);
				Bucket bucket = new Bucket(serialData, offset[i]);
				insertToBucket(bucket, value, recordOffset);
				long freePageNumber = bufferManager.getFreePageNumber(index
						.getIndexId());
				int offset = bufferManager.getFreeRecordOffset(
						index.getIndexId(), freePageNumber,
						index.getRecordsPerPage(), index.getRecordSize());
				bufferManager.write(index.getIndexId(), freePageNumber, offset,
						bucket.serialize());
				int recordNumber = (offset - (index.getRecordsPerPage() + 7) / 8)
						/ index.getRecordSize();
				bufferManager.writeRecordBitmap(index.getIndexId(),
						freePageNumber, index.getRecordsPerPage(),
						recordNumber, true);
			} else {
				split.returnCode = 1;
			}
			return split;
		}

		public void insertToBucket(Bucket bucket, PhysicalAddress value,
				int recordOffset) {
			// TODO Auto-generated method stub
			for (int i = 0; i < 50; i++) {
				if (bucket.offset[i] == -1) {
					bucket.pointers[i] = value;
					bucket.offset[i] = recordOffset;
					return;
				}
			}
			Bucket _bucket = new Bucket();
			insertToBucket(_bucket, value, recordOffset);
			long freePageNumber = bufferManager.getFreePageNumber(index
					.getIndexId());
			int offset = bufferManager.getFreeRecordOffset(index.getIndexId(),
					freePageNumber, index.getRecordsPerPage(),
					index.getRecordSize());
			bufferManager.write(index.getIndexId(), freePageNumber, offset,
					_bucket.serialize());
			int recordNumber = (offset - (index.getRecordsPerPage() + 7) / 8)
					/ index.getRecordSize();
			bufferManager.writeRecordBitmap(index.getIndexId(), freePageNumber,
					index.getRecordsPerPage(), recordNumber, true);
			bucket.nextBucket = new PhysicalAddress(index.getIndexId(),
					freePageNumber);
			bucket.nextBucketoffset = offset;
		}
	}

	class Bucket {
		public PhysicalAddress[] pointers;
		public int[] offset;

		public PhysicalAddress nextBucket;
		public int nextBucketoffset;

		public Bucket() {
			pointers = new PhysicalAddress[51];
			offset = new int[51];
		}

		public Bucket(ByteBuffer serializedData, int position) {
			pointers = new PhysicalAddress[51];
			offset = new int[51];
		}

		public ByteBuffer serialize() {
			return null;
		}
	}
	
	class Split {
		public Key key;
		public PhysicalAddress left;
		public long leftOffset;
		public PhysicalAddress right;
		public long rightOffset;
		public int returnCode;

		public Split(Key k, PhysicalAddress l, long lOffset, PhysicalAddress r, long rOffset) {
			key = k;
			left = l;
			leftOffset = lOffset;
			right = r;
			rightOffset = rOffset;
			returnCode = 0;
		}
	}

}
