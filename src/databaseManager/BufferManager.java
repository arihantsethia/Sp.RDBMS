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

/**
 * @param MAX_PAGE_COUNT
 *            maximum number of pages can be in buffer pool of main memory.
 *            clockTime used in replacement algorithm. isDirty used to tell
 *            whether a page is written by query or not. lookUpTable it gives
 *            extra information about pages lies on buffer pool like it returns
 *            block number corresponding to physical storage and also it tells
 *            about whether a page is PINNED or UNPINNED. lookUpMap it return
 *            logical address corresponding to given a physical address.
 *            diskSpaceManager object of class diskSpaceManager use to do low
 *            level operations like read or write blocks to disk. pagePool list
 *            of pages lies at main memory. openFiles return file descriptor
 *            corresponding to file id.
 */

public class BufferManager {

	/* This stores the maximum number of pages that can be in the main memory. */
	public static final int MAX_PAGE_COUNT = 4096;

	/**
	 * If page is in memory & pinned then set the TIME_INDEX of the page to -1
	 * If page is not present then the TIME_INDEX of the page = 0 If page is in
	 * memory then the TIME_INDEX > 0
	 */

	private boolean[] isDirty;
	private PhysicalAddress[] lookUpTable;
	private long[] clockTick;
	private Map<PhysicalAddress, Long> lookUpMap;
	private DiskSpaceManager diskSpaceManager;
	private ByteBuffer[] pagePool;
	private Map<Long, FileChannel> openFiles;
	private RelationHolder relationHolder;

	public BufferManager() {
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

	// initialize lookUpTable by default all pages will be free and they will
	// point to -1 block number.
	private void initializeTable() {
		for (int i = 0; i < MAX_PAGE_COUNT; i++) {
			isDirty[i] = false;
			lookUpTable[i] = new PhysicalAddress(-1, -1);
			clockTick[i] = 0;
		}
		lookUpMap.clear();
		openFiles.clear();
	}

	public void flush() {
		for (int i = 0; i < MAX_PAGE_COUNT; i++) {
			writePhysical(lookUpTable[i]);
		}
	}

	// return logical address of a block that is free. and if no block is free
	// then use some replacement algorithms to free that block.
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

	// assign a page in main memory to a new block retrieve from Disk Storage.
	private long addToPagePool(final PhysicalAddress physicalAddress,
			final ByteBuffer pageData) {
		long logicalAddress = getFreeBlock();
		pagePool[(int) logicalAddress] = pageData;
		lookUpTable[(int) logicalAddress] = physicalAddress;
		clockTick[(int) logicalAddress] = 1;
		lookUpMap.put(physicalAddress, logicalAddress);
		return logicalAddress;
	}

	// retrieve data from page in buffer pool given physicalAddress as argument.
	private ByteBuffer getPageFromPool(final PhysicalAddress physicalAddress) {
		if (lookUpMap.containsKey(physicalAddress)) {
			clockTick[lookUpMap.get(physicalAddress).intValue()]++;
			pagePool[lookUpMap.get(physicalAddress).intValue()].position(0);
			return pagePool[lookUpMap.get(physicalAddress).intValue()];
		} else {
			throw new Error("Trying to access undefined memory");
		}
	}

	// retrieve data from page in buffer pool given relation and block no. as
	// argument.
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
	private PhysicalAddress getPhysicalAddress(final long relationId,
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

	public boolean writeRecordBitmap(long relationId, long block,
			int recordNumber, int setValue) {
		PhysicalAddress physicalAddress = getPhysicalAddress(relationId, block);
		if (lookUpMap.containsKey(physicalAddress)) {
			isDirty[lookUpMap.get(physicalAddress).intValue()] = true;
			byte oldValue = pagePool[lookUpMap.get(physicalAddress).intValue()]
					.get(recordNumber / Byte.SIZE);
			BitSet bitSet = BitSet.valueOf(new byte[] { oldValue });
			bitSet.set(recordNumber % Byte.SIZE, (setValue == 1));
			pagePool[lookUpMap.get(physicalAddress).intValue()].put(
					recordNumber / Byte.SIZE, bitSet.toByteArray()[0]);
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
			for (int i = 1; i < DiskSpaceManager.BLOCK_SIZE
					* DiskSpaceManager.BLOCK_SIZE; i++) {
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
				System.out.println("Free Block :" + freeBlockNumber
						+ " Free record " + i + " offset :"
						+ (i * recordSize + (recordsPerBlock + 7) / 8));
				return i * recordSize + (recordsPerBlock + 7) / 8;
			}
		}
		return -1;
	}
}
