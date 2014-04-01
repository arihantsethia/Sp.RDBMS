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

	private long clockTime = 0;
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

	private void initializeTable() {
		for (int i = 0; i < MAX_PAGE_COUNT; i++) {
			isDirty[i] = false;
			lookUpTable[i].id = -1;
			clockTick[i] = 0;
		}
		lookUpMap.clear();
		openFiles.clear();
	}

	private long getFreeBlock() {
		int logicalPageNumber = 0;
		int distinctPinCount = 0;
		while (true) {
			if (clockTick[logicalPageNumber] == 0) {
				writePhysical(lookUpTable[logicalPageNumber]);
				break;
			} else {
				if (clockTick[logicalPageNumber] == -1) {
					distinctPinCount++;
					if (distinctPinCount == MAX_PAGE_COUNT) {
						throw new OutOfMemoryError("Out of memory");
					}
					/* DON'T DO ANYTHING */
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

	private long addToPagePool(final PhysicalAddress physicalAddress,
			final ByteBuffer pageData) {
		long logicalAddress = getFreeBlock();
		pagePool[(int) logicalAddress] = pageData;
		lookUpTable[(int) logicalAddress] = physicalAddress;
		clockTick[(int) logicalAddress] = 1;
		lookUpMap.put(physicalAddress, logicalAddress);
		return logicalAddress;
	}

	private ByteBuffer getPageFromPool(final PhysicalAddress physicalAddress) {
		if (lookUpMap.containsKey(physicalAddress)) {
			clockTick[lookUpMap.get(physicalAddress).intValue()]++;
			return pagePool[lookUpMap.get(physicalAddress).intValue()];
		} else {
			throw new Error("Trying to access undefined memory");
		}
	}

	private ByteBuffer getPageFromPool(final long relationId, final long block) {
		return getPageFromPool(getPhysicalAddress(relationId, block));
	}

	private boolean pinPage(final long logicalAddress) {
		if (clockTick[(int) logicalAddress] == -1) {
			return false;
		} else {
			clockTick[(int) logicalAddress] = -1;
			return true;
		}
	}

	private boolean pinPage(final long relationId, final long block) {
		PhysicalAddress physicalAddress = getPhysicalAddress(relationId, block);
		if (lookUpMap.containsKey(physicalAddress)) {
			return pinPage(lookUpMap.get(physicalAddress));
		} else {
			return false;
		}
	}

	private boolean unPinPage(final long logicalAddress) {
		if (clockTick[(int) logicalAddress] == -1) {
			clockTick[(int) logicalAddress] = 1;
			return true;
		} else {
			return false;
		}
	}

	private boolean unPinPage(final long relationId, final long block) {
		PhysicalAddress physicalAddress = getPhysicalAddress(relationId, block);
		if (lookUpMap.containsKey(physicalAddress)) {
			return unPinPage(lookUpMap.get(physicalAddress));
		} else {
			return false;
		}
	}

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
			ByteBuffer newPage = diskSpaceManager.read(openFiles.get(relationId),
					block);
			addToPagePool(getPhysicalAddress(relationId, block), newPage);
			return newPage;
		}
	}

	public boolean writePhysical(final PhysicalAddress physicalAddress) {
		if (lookUpMap.containsKey(physicalAddress)) {
			int logicalPageNumber = lookUpMap.get(physicalAddress).intValue();
			if (isDirty[logicalPageNumber]) {
				if (!openFiles.containsKey(physicalAddress.id)) {
					FileChannel newFileChannel = diskSpaceManager
							.openFile(relationHolder.getRelation(physicalAddress.id).getFileName());
					openFiles.put(physicalAddress.id, newFileChannel);
				}
				diskSpaceManager.write(openFiles.get(physicalAddress.id),
						physicalAddress.offset, pagePool[logicalPageNumber]);
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
			pagePool[lookUpMap.get(physicalAddress).intValue()].put(
					writeStream, blockSeek, writeStream.length);
		}
		return false;
	}

	public static ByteBuffer getEmptyBlock() {
		return ByteBuffer.allocateDirect((int) DiskSpaceManager.BLOCK_SIZE);
	}
}
