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
	 * openFiles is Map from objectId to FileChannel for the file containing the
	 * table.
	 */
	private Map<Long, FileChannel> openFiles;
	// lookUpTable is array of PhysicalAddress present in current page pool.
	private PhysicalAddress[] lookUpTable;
	// lookUpMap is a map from PhysicalAddresss to the index on lookUpTable.
	private Map<PhysicalAddress, Long> lookUpMap;
	// pagePool is an array of pages currently in main memory.
	private ByteBuffer[] pagePool;
	private ObjectHolder objectHolder;
	private DiskSpaceManager diskSpaceManager;

	/** For singleton use */
	private static BufferManager thisbuffer;

	private BufferManager() {
		diskSpaceManager = new DiskSpaceManager();
		isDirty = new boolean[MAX_PAGE_COUNT];
		lookUpTable = new PhysicalAddress[MAX_PAGE_COUNT];
		clockTick = new long[MAX_PAGE_COUNT];
		lookUpMap = new HashMap<PhysicalAddress, Long>();
		openFiles = new HashMap<Long, FileChannel>();
		pagePool = new ByteBuffer[MAX_PAGE_COUNT];
		objectHolder = ObjectHolder.getObjectHolder();
		initializeTable();
	}

	/**
	 * The Singleton accessor for BufferManagers. This is the only way to get a
	 * BufferManager.
	 * 
	 * @return The copy of BufferManager for the system.
	 */
	public synchronized static BufferManager getBufferManager() {
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
	public void initializeTable() {
		for (int i = 0; i < MAX_PAGE_COUNT; i++) {
			isDirty[i] = false;
			lookUpTable[i] = new PhysicalAddress(-1, -1);
			clockTick[i] = 0;
		}
		lookUpMap.clear();
		for (Map.Entry<Long, FileChannel> entry : openFiles.entrySet()) {
			diskSpaceManager.closeFile(openFiles.get(entry.getKey()));
		}
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
	 * This function returns the logical address of a free page. If no such page
	 * exists then it evicts is page use LFU replacement algorithm.
	 * 
	 * @return logical address of a page that is free
	 */
	public long getFreePage() {
		int logicalPageNumber = 0;
		int distinctPinCount = 0;
		int minTick = MAX_PAGE_COUNT;

		for (logicalPageNumber = 0; logicalPageNumber < MAX_PAGE_COUNT; logicalPageNumber++) {
			if (clockTick[logicalPageNumber] == 0) {
				writePhysical(lookUpTable[logicalPageNumber]);
				lookUpMap.remove(lookUpTable[logicalPageNumber]);
				return (long) logicalPageNumber;
			} else if (clockTick[logicalPageNumber] < 0) {
				distinctPinCount++;
			} else {
				minTick = (int) (clockTick[logicalPageNumber] < minTick ? clockTick[logicalPageNumber] : minTick);
			}
		}

		if (distinctPinCount == MAX_PAGE_COUNT) {
			throw new OutOfMemoryError("Out of memory");
		}

		for (logicalPageNumber = 0; logicalPageNumber < MAX_PAGE_COUNT; logicalPageNumber++) {
			if (clockTick[logicalPageNumber] > 0) {
				clockTick[logicalPageNumber] = clockTick[logicalPageNumber] - minTick;
			}
		}

		for (logicalPageNumber = 0; logicalPageNumber < MAX_PAGE_COUNT; logicalPageNumber++) {
			if (clockTick[logicalPageNumber] == 0) {
				writePhysical(lookUpTable[logicalPageNumber]);
				lookUpMap.remove(lookUpTable[logicalPageNumber]);
				return (long) logicalPageNumber;
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
	private long addToPagePool(final PhysicalAddress physicalAddress, final ByteBuffer pageData) {
		long logicalAddress = getFreePage();
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
	 * particular object, id.
	 * 
	 * @param objectId
	 *            : object table for which page needs to be fetched.
	 * @param page
	 *            : page number of the corresponding page
	 * @return The data stored on the particular page pointed by the objectId
	 *         and page number.
	 */
	private ByteBuffer getPageFromPool(final long objectId, final long page) {
		return getPageFromPool(getPhysicalAddress(objectId, page));
	}

	// PIN the page in main memory if it is not already PINNED.
	public boolean pinPage(final long logicalAddress) {
		if (clockTick[(int) logicalAddress] == -1) {
			return false;
		} else {
			clockTick[(int) logicalAddress] = -1;
			return true;
		}
	}

	// PIN the page in main memory if it is not already PINNED given object id
	// and page no. as argument.
	public boolean pinPage(final long objectId, final long page) {
		PhysicalAddress physicalAddress = getPhysicalAddress(objectId, page);
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

	// UNPIN the page in main memory if it is PINNED given object id and page
	// no. as arguments.
	public boolean unPinPage(final long objectId, final long page) {
		PhysicalAddress physicalAddress = getPhysicalAddress(objectId, page);
		if (lookUpMap.containsKey(physicalAddress)) {
			return unPinPage(lookUpMap.get(physicalAddress));
		} else {
			return false;
		}
	}

	// UNPIN all the pages in main memory which are PINNED.
	public void unPinAll() {
		for (int i = 0; i < MAX_PAGE_COUNT; i++) {
			unPinPage((long) i);
		}
	}

	public FileChannel openFile(long objectId) {
		FileChannel fileChannel = diskSpaceManager.openFile(objectHolder.getObjectFileName(objectId));
		openFiles.put(objectId, fileChannel);
		return fileChannel;
	}

	// given object id and page no. as argument find physical address of a
	// page.
	public static PhysicalAddress getPhysicalAddress(final long objectId, final long page) {
		return new PhysicalAddress(objectId, page * DiskSpaceManager.PAGE_SIZE);
	}

	private boolean isPinned(final PhysicalAddress physicalAddress) {
		if (isPresentInPool(physicalAddress)) {
			return (clockTick[lookUpMap.get(physicalAddress).intValue()] == -1);
		} else {
			return false;
		}
	}

	public boolean isPinned(final long objectId, final long page) {
		return isPinned(getPhysicalAddress(objectId, page));
	}

	private boolean isPresentInPool(final PhysicalAddress physicalAddress) {
		return lookUpMap.containsKey(physicalAddress);
	}

	private boolean isPresentInPool(final long objectId, final long page) {
		return isPresentInPool(getPhysicalAddress(objectId, page));
	}

	public ByteBuffer read(final long objectId, final long page) {
		if (isPresentInPool(objectId, page)) {
			return getPageFromPool(objectId, page);
		} else {
			if (!openFiles.containsKey(objectId)) {
				FileChannel newFileChannel = diskSpaceManager.openFile(objectHolder.getObjectFileName(objectId));
				openFiles.put(objectId, newFileChannel);
			}
			ByteBuffer newPage = diskSpaceManager.read(openFiles.get(objectId), page);
			addToPagePool(getPhysicalAddress(objectId, page), newPage);
			newPage.position(0);
			return newPage;
		}
	}

	public boolean writePhysical(final PhysicalAddress physicalAddress) {
		if (lookUpMap.containsKey(physicalAddress)) {
			int logicalPageNumber = lookUpMap.get(physicalAddress).intValue();
			if (isDirty[logicalPageNumber]) {
				if (!openFiles.containsKey(physicalAddress.id)) {
					FileChannel newFileChannel = diskSpaceManager.openFile(objectHolder.getObjectFileName(physicalAddress.id));
					openFiles.put(physicalAddress.id, newFileChannel);
				}
				pagePool[logicalPageNumber].position(0);
				diskSpaceManager.write(openFiles.get(physicalAddress.id), physicalAddress.offset / DiskSpaceManager.PAGE_SIZE, pagePool[logicalPageNumber]);
			}
			isDirty[logicalPageNumber] = false;
			return true;
		}
		return false;
	}

	public boolean write(final long objectId, final long page, final int pageSeek, final ByteBuffer writeBuffer) {
		PhysicalAddress physicalAddress = getPhysicalAddress(objectId, page);
		if (!lookUpMap.containsKey(physicalAddress)) {
			read(objectId, page);
		}
		isDirty[lookUpMap.get(physicalAddress).intValue()] = true;
		byte[] writeStream = new byte[writeBuffer.capacity()];
		writeBuffer.position(0);
		writeBuffer.get(writeStream);
		pagePool[lookUpMap.get(physicalAddress).intValue()].position(pageSeek);
		pagePool[lookUpMap.get(physicalAddress).intValue()].put(writeStream);
		return true;
	}

	public boolean writePageBitmap(long objectId, long pageNumber, boolean setValue) {
		long page = pageNumber / (DiskSpaceManager.PAGE_SIZE * Byte.SIZE);
		int pageMod = (int) (pageNumber % (DiskSpaceManager.PAGE_SIZE * Byte.SIZE));
		PhysicalAddress physicalAddress = getPhysicalAddress(objectId, page);
		if (!lookUpMap.containsKey(physicalAddress)) {
			read(objectId, page);
		}
		isDirty[lookUpMap.get(physicalAddress).intValue()] = true;
		byte oldValue = pagePool[lookUpMap.get(physicalAddress).intValue()].get((int) (pageMod / Byte.SIZE));
		BitSet bitSet = BitSet.valueOf(new byte[] { oldValue });
		bitSet.set((int) (pageMod % Byte.SIZE), setValue);
		byte newVal = 0;
		if (bitSet.toByteArray().length > 0) {
			newVal = bitSet.toByteArray()[0];
		}
		pagePool[lookUpMap.get(physicalAddress).intValue()].put(pageMod / Byte.SIZE, newVal);
		return true;
	}

	public boolean writeRecordBitmap(long objectId, long page, int recordsPerPage, int recordNumber, boolean setValue) {
		PhysicalAddress physicalAddress = getPhysicalAddress(objectId, page);
		if (!lookUpMap.containsKey(physicalAddress)) {
			read(objectId, page);
		}
		isDirty[lookUpMap.get(physicalAddress).intValue()] = true;
		ByteBuffer currentPage = pagePool[lookUpMap.get(physicalAddress).intValue()];
		byte oldValue = currentPage.get(recordNumber / Byte.SIZE);
		BitSet bitSet = BitSet.valueOf(new byte[] { oldValue });
		bitSet.set(recordNumber % Byte.SIZE, setValue);
		byte newVal = 0;
		if (bitSet.toByteArray().length > 0) {
			newVal = bitSet.toByteArray()[0];
		}
		pagePool[lookUpMap.get(physicalAddress).intValue()].put(recordNumber / Byte.SIZE, newVal);
		if (getFreeRecordOffset(objectId, page, recordsPerPage, 50) == -1) {
			writePageBitmap(objectId, page, true);
		}
		return true;

	}

	public long getFreePageNumber(long objectId) {
		byte[] freePages = new byte[(int) DiskSpaceManager.PAGE_SIZE];
		BitSet bitMapFreePages;
		long bitMapPageNumber = 0;
		while (true) {
			read(objectId, bitMapPageNumber).get(freePages);
			bitMapFreePages = BitSet.valueOf(freePages);
			for (int i = 1; i < DiskSpaceManager.PAGE_SIZE * Byte.SIZE; i++) {
				if (bitMapFreePages.get(i) == false) {
					return i + bitMapPageNumber;
				}
			}
			bitMapPageNumber = bitMapPageNumber + DiskSpaceManager.PAGE_SIZE * Byte.SIZE;
		}
	}

	public int getFreeRecordOffset(long objectId, long freePageNumber, int recordsPerPage, int recordSize) {
		byte[] byteFreeRecords = new byte[(int) (recordsPerPage + 7) / 8];
		read(objectId, freePageNumber).get(byteFreeRecords);
		BitSet bitMapFreeRecords = BitSet.valueOf(byteFreeRecords);
		for (int i = 0; i < recordsPerPage; i++) {
			if (bitMapFreeRecords.get(i) == false) {
				return i * recordSize + (recordsPerPage + 7) / 8;
			}
		}
		return -1;
	}

	public void closeFile(long indexId) {
		for (int i = 0; i < MAX_PAGE_COUNT; i++) {
			if (lookUpTable[i].id == indexId) {
				writePhysical(lookUpTable[i]);
				lookUpMap.remove(lookUpTable[i]);
				clockTick[i] = 0;
				lookUpTable[i] = new PhysicalAddress(-1, -1);
			}
		}
		if (openFiles.containsKey(indexId)) {
			diskSpaceManager.closeFile(openFiles.get(indexId));
			openFiles.remove(indexId);
		}
	}

	public boolean deleteFile(String fileName) {
		return diskSpaceManager.deleteFile(fileName);
	}
}
