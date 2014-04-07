package databaseManager;

import java.io.File;
import java.nio.channels.FileChannel;

public class BPlusTree {
	class Node {
		public int mNumKeys = 0;
		public int[] mKeys = new int[2 * d - 1];
		public Object[] mObjects = new Object[2 * d - 1];
		public Node[] mChildNodes = new Node[2*d];
		public boolean mIsLeafNode;
		public Node mNextNode;
	};
	
	private Node mRootNode;
	private static final int d = 4;
	private FileChannel fileChannel;
	private String fileName;
	private boolean duplicates;
	private BufferManager bufferManager;
	public BPlusTree(){
		mRootNode = new Node();
		mRootNode.mIsLeafNode = true;
	}
	
	public void add(int key){}
	
	
	public void insertIntoNonFullNode(Node node){}
	
	public void openIndex(final String _fileName, boolean _duplicates){
		fileName = _fileName;
		duplicates = _duplicates;
	}
	
	public void closeIndex(){
		
	}
}
