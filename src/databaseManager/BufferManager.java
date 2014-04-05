/**
 * 	@author Arihant , Arun and Vishavdeep
 *  Class BufferManager
 *  This class access methods of class DiskSpaceManager. and it's methods will be accessed by SystemCatalogManager class. 
 *   
 */

package databaseManager;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

/**
 *	@param
 *		MAX_PAGE_COUNT
 *			maximum number of pages can be in buffer pool of main memory.
 *		PHYSICAL_INDEX
 *			one of the argument of look up table used for returning block number.
 *		TIME_INDEX
 *			one of the argument of look up table used for return status of block like PINNED , UNPINNED or FREE.
 * 			If page is in memory & pinned then set the TIME_INDEX of the page to -1 If page is
 * 			not present then the TIME_INDEX of the page = 0 If page is in memory then the TIME_INDEX > 0
 *		clockTime
 *			used in replacement algorithm.
 *		isDirty
 *			used to tell whether a page is written by query or not.
 *      lookUpTable
 *      	it gives extra information about pages lies on buffer pool like it returns block number corresponding to physical storage and 
 *      	also it tells about whether a page is PINNED or UNPINNED.
 *      lookUpMap
 *      	it return logical address corresponding to given a physical address.
 *      diskSpaceManager
 *      	object of class diskSpaceManager use to do low level operations like read or write blocks to disk.
 *      pagePool
 *      	list of pages lies at main memory.
 *      openFiles
 *      	return file descriptor corresponding to file id.
 */

public class BufferManager {

	/* This stores the maximum number of pages that can be in the main memory. */
	public static final int MAX_PAGE_COUNT = 4096;
	public static final int PHYSICAL_INDEX = 0;
	public static final int TIME_INDEX = 1;

	private long clockTime = 0;
	private boolean[] isDirty;
	private long[][] lookUpTable;
	private Map<Long, Long> lookUpMap;
	private DiskSpaceManager diskSpaceManager;
	private ByteBuffer[] pagePool;
	private Map<Long,FileChannel> openFiles;
	
	// Constructor to initiate object instance and initialize variables.
	public BufferManager() {
		diskSpaceManager = new DiskSpaceManager();
		isDirty = new boolean[MAX_PAGE_COUNT];
		lookUpTable = new long[MAX_PAGE_COUNT][2];
		lookUpMap = new HashMap<Long, Long>();
		openFiles = new HashMap<Long,FileChannel>();
		pagePool = new ByteBuffer[MAX_PAGE_COUNT];
		initializeTable();
	}

	// initialize lookUpTable by default all pages will be free and they will point to -1 block number.
	private void initializeTable() {
		for (int i = 0; i < MAX_PAGE_COUNT; i++) {
			isDirty[i] = false;
			lookUpTable[i][PHYSICAL_INDEX] = -1;
			lookUpTable[i][TIME_INDEX] = 0;
		}
		lookUpMap.clear();
		openFiles.clear();
	}

	// return logical address of a block that is free. and if no block is free then use some replacement algorithms to free that block. 
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

	// assign a page in main memory to a new block retrieve from Disk Storage.
	private long addToPagePool(final long physicalAddress,final ByteBuffer pageData) {
		long logicalAddress = getFreeBlock();
		pagePool[(int) logicalAddress] = pageData;
		lookUpTable[(int) logicalAddress][PHYSICAL_INDEX] = physicalAddress;
		lookUpTable[(int) logicalAddress][TIME_INDEX] = 1;
		lookUpMap.put(physicalAddress, logicalAddress);
		return logicalAddress;
	}

	// retrieve data from page in buffer pool given physicalAddress as argument.
	private ByteBuffer getPageFromPool(final long physicalAddress) {
		if (lookUpMap.containsKey(physicalAddress)) {
			lookUpTable[lookUpMap.get(physicalAddress).intValue()][TIME_INDEX]++;
			return pagePool[lookUpMap.get(physicalAddress).intValue()];
		} else {
			throw new Error("Trying to access undefined memory");
		}
	}
	
	// retrieve data from page in buffer pool given relation and block no. as argument.
	private ByteBuffer getPageFromPool(final long relation, final long block) {
		return getPageFromPool(getPhysicalAddress(relation,block));
	}

	// PIN the page in main memory if it is not already PINNED.
	private boolean pinPage(final long logicalAddress) {
		if (lookUpTable[(int) logicalAddress][TIME_INDEX] == -1) {
			return false;
		} else {
			lookUpTable[(int) logicalAddress][TIME_INDEX] = -1;
			return true;
		}
	}

	// PIN the page in main memory if it is not already PINNED given relation id and block no. as argument.
	private boolean pinPage(final long relation, final long block) {
		long physicalAddress = getPhysicalAddress(relation, block);
		if (lookUpMap.containsKey(physicalAddress)) {
			return pinPage(lookUpMap.get(physicalAddress));
		} else {
			return false;
		}
	}

	// UNPIN the page in main memory if it is PINNED.
	private boolean unPinPage(final long logicalAddress) {
		if (lookUpTable[(int) logicalAddress][TIME_INDEX] == -1) {
			lookUpTable[(int) logicalAddress][TIME_INDEX] = 1;
			return true;
		} else {
			return false;
		}
	}

	// UNPIN the page in main memory if it is PINNED given relation id and block no. as arguments.
	private boolean unPinPage(final long relation, final long block) {
		long physicalAddress = getPhysicalAddress(relation, block);
		if (lookUpMap.containsKey(physicalAddress)) {
			return unPinPage(lookUpMap.get(physicalAddress));
		} else {
			return false;
		}
	}

	// given relation id and block no. as argument find physical address of a block.
	private long getPhysicalAddress(final long relation, final long block) {
		long physicalAddress = 0;
		// Need to implement this
		return physicalAddress;
	}

	//
	private boolean isPinned(final long physicalAddress) {
		if (isPresentInPool(physicalAddress)) {
			return (lookUpTable[lookUpMap.get(physicalAddress).intValue()][TIME_INDEX] == -1);
		} else {
			return false;
		}
	}

	private boolean isPinned(final long relation, final long block) {
		return isPinned(getPhysicalAddress(relation, block));
	}

	private boolean isPresentInPool(final long physicalAddress) {
		return lookUpMap.containsKey(physicalAddress);
	}

	private boolean isPresentInPool(final long relation, final long block) {
		return isPresentInPool(getPhysicalAddress(relation, block));
	}
	
	public ByteBuffer read(final long relation, final long block){
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
	
	public ByteBuffer write(final long relation, final long block){
		//Write to the logical address;
		//isDirty[physicalAddress] = true; 
		return null;
	}

	public static ByteBuffer getEmptyBlock() {
		return ByteBuffer.allocateDirect((int) DiskSpaceManager.BLOCK_SIZE);
	}
}
