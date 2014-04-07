/**
 * 	@author Arihant , Arun and Vishavdeep
 *  Class BufferManager
 *  This class access methods of class DiskSpaceManager. and it's methods will be accessed by SystemCatalogManager class. 
 *   
 */

package databaseManager;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class BufferManager {

	/* This stores the maximum number of pages that can be in the main memory. */
	public static final int MAX_PAGE_COUNT = 4096;
	/*
	 * isDirty used to tell whether a page is written by query or not.If isDirty
	 * is set to true then the page needs to be written to physical memory.
	 */
	private boolean[] isDirty;
	/*
	 * clockTick maintains the access frequency of each page. It is set to -1 is
	 * a particular page is pinned. For a page in memory clockTick > 0 and if
	 * clockTick = 0 then page can be evicted by page replacment algorithm.
	 */
	private long[] clockTick;
	/*
	 * openFiles is Map from relationId to FileChannel for the file containing
	 * the table.
	 */
	private Map<Long, FileChannel> openFiles;
	// lookUpTable is array of PhysicalAddress present in current page pool.
	private PhysicalAddress[] lookUpTable;
	// lookUpMap is a map from PhysicalAddresss to the index on lookUpTable.
	private Map<PhysicalAddress, Long> lookUpMap;
	// pagePool is an array of pages currently in main memory.
	private ByteBuffer[] pagePool;
	private RelationHolder relationHolder;
	private DiskSpaceManager diskSpaceManager;

	/**For singleton use*/
	private static BufferManager thisbuffer;
	
	private BufferManager() {
		diskSpaceManager = new DiskSpaceManager();
		isDirty = new boolean[MAX_PAGE_COUNT];
		lookUpTable = new PhysicalAddress[MAX_PAGE_COUNT];
		clockTick = new long[MAX_PAGE_COUNT];
		lookUpMap = new HashMap<PhysicalAddress, Long>();
		openFiles = new HashMap<Long, FileChannel>();
		pagePool = new ByteBuffer[MAX_PAGE_COUNT];
		relationHolder = RelationHolder.getRelationHolder();
		initializeTable();
	}
		 
    /**
     * The Singleton accessor for BufferManagers.  This is the only way to get a BufferManager.
     * @return The copy of BufferManager for the system.
     */
    public static BufferManager getBufferManager() {
    	if (thisbuffer != null) {
    		return thisbuffer;
    	} else {
    		thisbuffer = new BufferManager();
    		return thisbuffer;
    	}
    }

	/**
	 * initializes all the attributes of bufferManager to its default values.
	 */
	private void initializeTable() {
		for (int i = 0; i < MAX_PAGE_COUNT; i++) {
			isDirty[i] = false;
			lookUpTable[i] = new PhysicalAddress(-1, -1);
			clockTick[i] = 0;
		}
		lookUpMap.clear();
		openFiles.clear();
	}

	/**
	 * This function clears the page pool and writes all the pages back to
	 * physical memory
	 */
	public void flush() {
		for (int i = 0; i < MAX_PAGE_COUNT; i++) {
			writePhysical(lookUpTable[i]);
		}
	}

	/**
	 * This function returns the logical address of a free page. If no such
	 * block exists then it evicts is page use LFU replacement algorithm.
	 * 
	 * @return logical address of a block that is free
	 */
	private long getFreeBlock() {
		int logicalPageNumber = 0;
		int distinctPinCount = 0;
		while (true) {
			if (clockTick[logicalPageNumber] == 0) {
				writePhysical(lookUpTable[logicalPageNumber]);
				lookUpMap.remove(lookUpTable[logicalPageNumber]);
				break;
			} else {
				if (clockTick[logicalPageNumber] == -1) {
					distinctPinCount++;
					if (distinctPinCount == MAX_PAGE_COUNT) {
						throw new OutOfMemoryError("Out of memory");
					}
				} else {
					clockTick[logicalPageNumber]--;
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

	/**
	 * Adds a page to the current page pool
	 * 
	 * @param physicalAddress
	 *            : Physical Address of page.
	 * @param pageData
	 *            : Data stored on the page
	 * @return the logical address where this page has been stored
	 */
	private long addToPagePool(final PhysicalAddress physicalAddress,
			final ByteBuffer pageData) {
		long logicalAddress = getFreeBlock();
		pagePool[(int) logicalAddress] = pageData;
		lookUpTable[(int) logicalAddress] = physicalAddress;
		clockTick[(int) logicalAddress] = 1;
		lookUpMap.put(physicalAddress, logicalAddress);
		return logicalAddress;
	}

	/**
	 * This function retrieves page date from page pool corresponding to
	 * particular physicalAddress.
	 * 
	 * @param physicalAddress
	 *            : PhysicalAddress of page to be fetched from pagePool.
	 * @return The data stored on the particular page pointed by the
	 *         PhysicalAddress.
	 */
	private ByteBuffer getPageFromPool(final PhysicalAddress physicalAddress) {
		if (lookUpMap.containsKey(physicalAddress)) {
			clockTick[lookUpMap.get(physicalAddress).intValue()]++;
			pagePool[lookUpMap.get(physicalAddress).intValue()].position(0);
			return pagePool[lookUpMap.get(physicalAddress).intValue()];
		} else {
			throw new Error("Trying to access undefined memory");
		}
	}

	/**
	 * This function retrieves page date from page pool corresponding to
	 * particular relation, id.
	 * 
	 * @param relationId
	 *            : relation table for which page needs to be fetched.
	 * @param block
	 *            : page number of the corresponding page
	 * @return The data stored on the particular page pointed by the relationId
	 *         and page number.
	 */
	private ByteBuffer getPageFromPool(final long relationId, final long block) {
		return getPageFromPool(getPhysicalAddress(relationId, block));
	}

	// PIN the page in main memory if it is not already PINNED.
	private boolean pinPage(final long logicalAddress) {
		if (clockTick[(int) logicalAddress] == -1) {
			return false;
		} else {
			clockTick[(int) logicalAddress] = -1;
			return true;
		}
	}

	// PIN the page in main memory if it is not already PINNED given relation id
	// and block no. as argument.
	private boolean pinPage(final long relationId, final long block) {
		PhysicalAddress physicalAddress = getPhysicalAddress(relationId, block);
		if (lookUpMap.containsKey(physicalAddress)) {
			return pinPage(lookUpMap.get(physicalAddress));
		} else {
			return false;
		}
	}

	// UNPIN the page in main memory if it is PINNED.
	private boolean unPinPage(final long logicalAddress) {
		if (clockTick[(int) logicalAddress] == -1) {
			clockTick[(int) logicalAddress] = 1;
			return true;
		} else {
			return false;
		}
	}

	// UNPIN the page in main memory if it is PINNED given relation id and block
	// no. as arguments.
	private boolean unPinPage(final long relationId, final long block) {
		PhysicalAddress physicalAddress = getPhysicalAddress(relationId, block);
		if (lookUpMap.containsKey(physicalAddress)) {
			return unPinPage(lookUpMap.get(physicalAddress));
		} else {
			return false;
		}
	}

	// given relation id and block no. as argument find physical address of a
	// block.
	public static PhysicalAddress getPhysicalAddress(final long relationId,
			final long block) {
		return new PhysicalAddress(relationId, block
				* DiskSpaceManager.BLOCK_SIZE);
	}

	private boolean isPinned(final PhysicalAddress physicalAddress) {
		if (isPresentInPool(physicalAddress)) {
			return (clockTick[lookUpMap.get(physicalAddress).intValue()] == -1);
		} else {
			return false;
		}
	}

	private boolean isPinned(final long relationId, final long block) {
		return isPinned(getPhysicalAddress(relationId, block));
	}

	private boolean isPresentInPool(final PhysicalAddress physicalAddress) {
		return lookUpMap.containsKey(physicalAddress);
	}

	private boolean isPresentInPool(final long relationId, final long block) {
		return isPresentInPool(getPhysicalAddress(relationId, block));
	}

	public ByteBuffer read(final long relationId, final long block) {
		if (isPresentInPool(relationId, block)) {
			return getPageFromPool(relationId, block);
		} else {
			if (!openFiles.containsKey(relationId)) {
				FileChannel newFileChannel = diskSpaceManager
						.openFile(relationHolder.getRelation(relationId)
								.getFileName());
				openFiles.put(relationId, newFileChannel);
			}
			ByteBuffer newPage = diskSpaceManager.read(
					openFiles.get(relationId), block);
			addToPagePool(getPhysicalAddress(relationId, block), newPage);
			newPage.position(0);
			return newPage;
		}
	}

	public boolean writePhysical(final PhysicalAddress physicalAddress) {
		if (lookUpMap.containsKey(physicalAddress)) {
			int logicalPageNumber = lookUpMap.get(physicalAddress).intValue();
			if (isDirty[logicalPageNumber]) {
				if (!openFiles.containsKey(physicalAddress.id)) {
					FileChannel newFileChannel = diskSpaceManager
							.openFile(relationHolder.getRelation(
									physicalAddress.id).getFileName());
					openFiles.put(physicalAddress.id, newFileChannel);
				}
				pagePool[logicalPageNumber].position(0);
				diskSpaceManager.write(openFiles.get(physicalAddress.id),
						physicalAddress.offset / DiskSpaceManager.BLOCK_SIZE,
						pagePool[logicalPageNumber]);
			}
			isDirty[logicalPageNumber] = false;
			return true;
		}
		return false;
	}

	public boolean write(final long relationId, final long block,
			final int blockSeek, final ByteBuffer writeBuffer) {
		PhysicalAddress physicalAddress = getPhysicalAddress(relationId, block);
		if (lookUpMap.containsKey(physicalAddress)) {
			isDirty[lookUpMap.get(physicalAddress).intValue()] = true;
			byte[] writeStream = new byte[writeBuffer.capacity()];
			writeBuffer.position(0);
			writeBuffer.get(writeStream);
			pagePool[lookUpMap.get(physicalAddress).intValue()]
					.position(blockSeek);
			pagePool[lookUpMap.get(physicalAddress).intValue()]
					.put(writeStream);
			return true;
		}
		return false;
	}

	public boolean writeBlockBitmap(long relationId, long blockNumber,
			boolean setValue) {
		long block = blockNumber / (DiskSpaceManager.BLOCK_SIZE * Byte.SIZE);
		int blockMod = (int) (blockNumber % (DiskSpaceManager.BLOCK_SIZE * Byte.SIZE));
		PhysicalAddress physicalAddress = getPhysicalAddress(relationId, block);
		if (lookUpMap.containsKey(physicalAddress)) {
			isDirty[lookUpMap.get(physicalAddress).intValue()] = true;
			byte oldValue = pagePool[lookUpMap.get(physicalAddress).intValue()]
					.get((int) (blockMod / Byte.SIZE));
			BitSet bitSet = BitSet.valueOf(new byte[] { oldValue });
			bitSet.set((int) (blockMod % Byte.SIZE), setValue);
			pagePool[lookUpMap.get(physicalAddress).intValue()].put(blockMod
					/ Byte.SIZE, bitSet.toByteArray()[0]);
			return true;
		}
		return false;
	}

	public boolean writeRecordBitmap(long relationId, long block,
			int recordsPerBlock, int recordNumber, boolean setValue) {
		PhysicalAddress physicalAddress = getPhysicalAddress(relationId, block);
		if (lookUpMap.containsKey(physicalAddress)) {
			isDirty[lookUpMap.get(physicalAddress).intValue()] = true;
			ByteBuffer currentBlock = pagePool[lookUpMap.get(physicalAddress).intValue()];
			byte oldValue = currentBlock.get(recordNumber / Byte.SIZE);
			BitSet bitSet = BitSet.valueOf(new byte[] { oldValue });
			bitSet.set(recordNumber % Byte.SIZE, setValue);
			pagePool[lookUpMap.get(physicalAddress).intValue()].put(
					recordNumber / Byte.SIZE, bitSet.toByteArray()[0]);
			if(getFreeRecordOffset(relationId,block,recordsPerBlock,50) == -1){
				writeBlockBitmap(relationId, block, true);
			}
			return true;
		}
		return false;
	}

	public long getFreeBlockNumber(long relationId) {
		byte[] freeBlocks = new byte[(int) DiskSpaceManager.BLOCK_SIZE];
		BitSet bitMapFreeBlocks;
		long bitMapBlockNumber = 0;
		while (true) {
			read(relationId, bitMapBlockNumber).get(freeBlocks);
			bitMapFreeBlocks = BitSet.valueOf(freeBlocks);
			for (int i = 1; i < DiskSpaceManager.BLOCK_SIZE * Byte.SIZE; i++) {
				if (bitMapFreeBlocks.get(i) == false) {
					return i + bitMapBlockNumber;
				}
			}
			bitMapBlockNumber = bitMapBlockNumber + DiskSpaceManager.BLOCK_SIZE
					* Byte.SIZE;
		}
	}

	public int getFreeRecordOffset(long relationId, long freeBlockNumber,
			int recordsPerBlock, int recordSize) {
		byte[] byteFreeRecords = new byte[(int) (recordsPerBlock + 7) / 8];
		read(relationId, freeBlockNumber).get(byteFreeRecords);
		BitSet bitMapFreeRecords = BitSet.valueOf(byteFreeRecords);
		for (int i = 0; i < recordsPerBlock; i++) {
			if (bitMapFreeRecords.get(i) == false) {
				return i * recordSize + (recordsPerBlock + 7) / 8;
			}
		}
		return -1;
	}
}
