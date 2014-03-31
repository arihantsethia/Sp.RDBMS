/**
 * 
 */
package databaseManager;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * @author arihant
 * 
 */
public class BufferManager {

	/* This stores the maximum number of pages that can be in the main memory. */
	public static final int MAX_PAGE_COUNT = 4096;

	public static final int PHYSICAL_INDEX = 0;
	public static final int TIME_INDEX = 1;

	private long clockTime = 0;
	/**
	 * If page is pinned then set the TIME_INDEX of the page to -1 If page is
	 * not present then the TIME_INDEX of the page = 0 If page is in memory then
	 * the TIME_INDEX > 0
	 */

	private boolean[] isDirty;
	private long[][] lookUpTable;
	private Map<Long, Long> lookUpMap;
	private DiskSpaceManager diskSpaceManager;
	private ByteBuffer[] pagePool;
	private Map<Integer,FileChannel> openFiles;
	public BufferManager() {
		diskSpaceManager = new DiskSpaceManager();
		isDirty = new boolean[MAX_PAGE_COUNT];
		lookUpTable = new long[MAX_PAGE_COUNT][2];
		lookUpMap = new HashMap<Long, Long>();
		openFiles = new HashMap<Integer,FileChannel>();
		pagePool = new ByteBuffer[MAX_PAGE_COUNT];
		initializeTable();
	}

	private void initializeTable() {
		for (int i = 0; i < MAX_PAGE_COUNT; i++) {
			isDirty[i] = false;
			lookUpTable[i][PHYSICAL_INDEX] = -1;
			lookUpTable[i][TIME_INDEX] = 0;
		}
		lookUpMap.clear();
		openFiles.clear();
	}

	private long getFreeBlock() {
		int logicalPageNumber = 0;
		int distinctPinCount = 0;
		while (true) {
			if (lookUpTable[logicalPageNumber][TIME_INDEX] == 0) {
				if(isDirty[logicalPageNumber]){
					//Wrtie to storage.
				}
				break;
			} else {
				if (lookUpTable[logicalPageNumber][TIME_INDEX] == -1) {
					distinctPinCount++;
					if (distinctPinCount == MAX_PAGE_COUNT) {
						throw new OutOfMemoryError("Out of memory");
					}
					/* DON'T DO ANYTHING */
				} else {
					lookUpTable[logicalPageNumber][TIME_INDEX]--;
				}
			}
			logicalPageNumber++;
			if (logicalPageNumber == MAX_PAGE_COUNT) {
				logicalPageNumber = 0;
				distinctPinCount = 0;
			}
		}
		return (long) logicalPageNumber;
	}

	private long addToPagePool(final long physicalAddress,
			final ByteBuffer pageData) {
		long logicalAddress = getFreeBlock();
		pagePool[(int) logicalAddress] = pageData;
		lookUpTable[(int) logicalAddress][PHYSICAL_INDEX] = physicalAddress;
		lookUpTable[(int) logicalAddress][TIME_INDEX] = 1;
		lookUpMap.put(physicalAddress, logicalAddress);
		return logicalAddress;
	}

	private ByteBuffer getPageFromPool(final long physicalAddress) {
		if (lookUpMap.containsKey(physicalAddress)) {
			lookUpTable[lookUpMap.get(physicalAddress).intValue()][TIME_INDEX]++;
			return pagePool[lookUpMap.get(physicalAddress).intValue()];
		} else {
			throw new Error("Trying to access undefined memory");
		}
	}
	
	private ByteBuffer getPageFromPool(final int relation, final long block) {
		return getPageFromPool(getPhysicalAddress(relation,block));
	}

	private boolean pinPage(final long logicalAddress) {
		if (lookUpTable[(int) logicalAddress][TIME_INDEX] == -1) {
			return false;
		} else {
			lookUpTable[(int) logicalAddress][TIME_INDEX] = -1;
			return true;
		}
	}

	private boolean pinPage(final int relation, final long block) {
		long physicalAddress = getPhysicalAddress(relation, block);
		if (lookUpMap.containsKey(physicalAddress)) {
			return pinPage(lookUpMap.get(physicalAddress));
		} else {
			return false;
		}
	}

	private boolean unPinPage(final long logicalAddress) {
		if (lookUpTable[(int) logicalAddress][TIME_INDEX] == -1) {
			lookUpTable[(int) logicalAddress][TIME_INDEX] = 1;
			return true;
		} else {
			return false;
		}
	}

	private boolean unPinPage(final int relation, final long block) {
		long physicalAddress = getPhysicalAddress(relation, block);
		if (lookUpMap.containsKey(physicalAddress)) {
			return unPinPage(lookUpMap.get(physicalAddress));
		} else {
			return false;
		}
	}

	private long getPhysicalAddress(final int relation, final long block) {
		long physicalAddress = 0;
		// Need to implement this
		return physicalAddress;
	}

	private boolean isPinned(final long physicalAddress) {
		if (isPresentInPool(physicalAddress)) {
			return (lookUpTable[lookUpMap.get(physicalAddress).intValue()][TIME_INDEX] == -1);
		} else {
			return false;
		}
	}

	private boolean isPinned(final int relation, final long block) {
		return isPinned(getPhysicalAddress(relation, block));
	}

	private boolean isPresentInPool(final long physicalAddress) {
		return lookUpMap.containsKey(physicalAddress);
	}

	private boolean isPresentInPool(final int relation, final long block) {
		return isPresentInPool(getPhysicalAddress(relation, block));
	}
	
	public ByteBuffer read(final int relation, final long block){
		if(isPresentInPool(relation,block)){
			return getPageFromPool(relation,block);
		}else{
			if(!openFiles.containsKey(relation)){
				FileChannel newFileChannel = diskSpaceManager.openFile("asd"); 
				openFiles.put(relation,newFileChannel);
			}
			ByteBuffer newPage = diskSpaceManager.read(openFiles.get(relation), block);
			addToPagePool(getPhysicalAddress(relation,block),newPage);
			return newPage;
		}
	}
	
	public ByteBuffer write(final int relation, final long block){
		//Write to the logical address;
		//isDirty[physicalAddress] = true; 
		return null;
	}

	public static ByteBuffer getEmptyBlock() {
		return ByteBuffer.allocateDirect((int) DiskSpaceManager.BLOCK_SIZE);
	}
}
