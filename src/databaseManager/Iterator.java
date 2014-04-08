package databaseManager;

import java.nio.ByteBuffer;
import java.util.BitSet;

public class Iterator {
	private Relation relation;
	private int position;
	private long currentPage;
	private long nextRecord;
	private byte[] bitMapBytes;
	private byte[] recordEntry;
	private BitSet bitMapRecords;
	private ByteBuffer currentBuffer;
	BufferManager bufferManager;
	
	public Iterator() {

	}

	/**
	 * Creates a new instance of Iterator that will work on the specified
	 * relation.
	 * 
	 * @param newRelation
	 *            The relation who's records the new Iterator will fetch.
	 */
	public Iterator(final Relation newRelation) {
		relation = newRelation;
		currentPage = 0;
		nextRecord = 0;
		bitMapBytes = new byte[(int) (relation.getRecordsPerPage() + 7) / 8];
		recordEntry = new byte[(int) relation.getRecordSize()];
		bufferManager = BufferManager.getBufferManager();
	}

	/**
	 * This method will close the iterator.
	 * @return Whether or not the close was successful.
	 */
	public boolean close() {
		// TODO close the Iterator.
		return true;
	}

	public ByteBuffer getNext() {
		long relationFileSize = relation.getFileSize();
		while(relationFileSize >= currentPage*DiskSpaceManager.PAGE_SIZE ){
			if (currentPage % (DiskSpaceManager.PAGE_SIZE * Byte.SIZE) == 0) {
				currentPage++;
			}
			if(nextRecord == 0){
				currentBuffer = bufferManager.read(relation.getRelationId(), currentPage);
				currentBuffer.get(bitMapBytes);
				position = currentBuffer.position();
				bitMapRecords = BitSet.valueOf(bitMapBytes);
			}
			for(;nextRecord < bitMapRecords.length();){
				if(bitMapRecords.get((int) nextRecord)){
					currentBuffer.position(position);
					currentBuffer.get(recordEntry);
					position += relation.getRecordSize();
					nextRecord++;
					return ByteBuffer.wrap(recordEntry);
				}else{
					nextRecord++;
					position += relation.getRecordSize();
				}				
			}
			nextRecord = 0;
			currentPage++;
		}
		return null;
	}

	/**
	 * Returns if there is another record in the Relation.
	 * 
	 * @return If there is another record.
	 */
	public boolean hasNext() {
		long pageTotal = relation.getFileSize() / DiskSpaceManager.PAGE_SIZE;
		if (currentPage >= pageTotal) {
			return false;
		} else if (currentPage == pageTotal - 1) {
			if (nextRecord >= relation.getRecordsPerPage()) {
				return false;
			} else {
				return true;
			}
		} else {
			return true;
		}
	}

	public PhysicalAddress getAddress() {
		return BufferManager.getPhysicalAddress(relation.getRelationId(),
				currentPage);
	}

}
