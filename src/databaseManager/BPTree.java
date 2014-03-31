package databaseManager;

/*interface config {
 public static final long BLOCKSIZE = 4096;

 }*/

class Node {
	short d;
	short leafNode;
	short keyCount;
	long []keys;
	long []pointers;
	
	public Node(){		
	}
	
	public Node(short _d){
		d = _d;
		keys = new long[d - 1];
		pointers = new long[d];
	}
}

public class BPTree {

}
