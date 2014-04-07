package databaseManager;

import java.nio.ByteBuffer;
import java.util.BitSet;

public class Iterator {
	private Relation relation;
	private int position;
	private long currentBlock;
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
		currentBlock = 0;
		nextRecord = 0;
		bitMapBytes = new byte[(int) (relation.getRecordsPerBlock() + 7) / 8];
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

	public byte[] getNext() {
		long relationFileSize = relation.getFileSize();
		while(relationFileSize >= currentBlock*DiskSpaceManager.BLOCK_SIZE ){
			if (currentBlock % (DiskSpaceManager.BLOCK_SIZE * Byte.SIZE) == 0) {
				currentBlock++;
			}
			if(nextRecord == 0){
				currentBuffer = bufferManager.read(relation.getRelationId(), currentBlock);
				currentBuffer.get(bitMapBytes);
				position = currentBuffer.get();
				bitMapRecords = BitSet.valueOf(bitMapBytes);
			}
			for(;nextRecord < bitMapRecords.length();){
				if(bitMapRecords.get((int) nextRecord)){
					currentBuffer.position(position);
					currentBuffer.get(recordEntry);
					position += relation.getRecordSize();
					nextRecord++;
					return recordEntry;
				}else{
					nextRecord++;
					position += relation.getRecordSize();
				}				
			}
			nextRecord = 0;
		}
		return null;
	}

	/**
	 * Returns if there is another record in the Relation.
	 * 
	 * @return If there is another record.
	 */
	public boolean hasNext() {
		long Blocktotal = relation.getFileSize() / DiskSpaceManager.BLOCK_SIZE;
		if (currentBlock > Blocktotal - 1) {
			return false;
		} else if (currentBlock == Blocktotal - 1) {
			if (nextRecord >= relation.getRecordsPerBlock()) {
				return false;
			} else {
				return true;
			}
		} else {
			return false;
		}
	}

	public PhysicalAddress getAddress() {
		return BufferManager.getPhysicalAddress(relation.getRelationId(),
				currentBlock);
	}

}
