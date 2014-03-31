/**
 * 
 */
package databaseManager;

import java.nio.ByteBuffer;

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
	private long[][] lookUpTable;
	private DiskSpaceManager diskSpaceManager;
	private ByteBuffer[] pagePool;

	public BufferManager() {
		diskSpaceManager = new DiskSpaceManager();
		lookUpTable = new long[MAX_PAGE_COUNT][2];
		pagePool = new ByteBuffer[MAX_PAGE_COUNT];
		initializeTable();
	}

	private void initializeTable() {
		for (int i = 0; i < MAX_PAGE_COUNT; i++) {
			lookUpTable[i][PHYSICAL_INDEX] = -1;
			lookUpTable[i][TIME_INDEX] = 0;
		}
	}

	private long getFreeBlock() {
		int logicalPageNumber = 0;
		while (true) {
			if (lookUpTable[logicalPageNumber][TIME_INDEX] == 0) {
				break;
			} else {
				if (lookUpTable[logicalPageNumber][TIME_INDEX] == -1) {
					/* DON'T DO ANYTHING */
				} else {
					lookUpTable[logicalPageNumber][TIME_INDEX]--;
				}
			}
			logicalPageNumber++;
			if (logicalPageNumber == MAX_PAGE_COUNT) {
				logicalPageNumber = 0;
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
		return logicalAddress;
	}

	private boolean pinPage(final long logicalAddress) {
		if (lookUpTable[(int) logicalAddress][TIME_INDEX] == -1) {
			return false;
		} else {
			lookUpTable[(int) logicalAddress][TIME_INDEX] = -1;
			return true;
		}
	}

}
