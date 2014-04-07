package databaseManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class Iterator {

	private  Relation relation ;
	private long currentBlock = 0;
	private long nextRecord   = 0;
	BufferManager bufferManager = BufferManager.getBufferManager(); 
	
	public Iterator() {
		
	}
	
	/**Creates a new instance of Iterator that will work on the specified
	 * relation.
	 * @param newRelation The relation who's records the new Iterator will 
	 * fetch.
	 */
	public Iterator(final Relation newRelation) {
		this.relation = newRelation;
		System.out.println(relation); //TODO delete later, used to remove warning about not using relation.
	}
	
	/**This method will close the iterator.
	 * @return Whether or not the close was successful.
	 */
	public boolean close() {
		//TODO close the Iterator.
		return true;
	}
	
	public byte[] getNext() {
		byte[] recordEntry = new byte[(int)relation.getRecordSize()];
		long relationFileSize = relation.getFileSize();
		long bitMapBlockNumber = 0;
		long recordsPerBlock = relation.getRecordsPerBlock();
		byte[] blocksMap = new byte[(int) DiskSpaceManager.BLOCK_SIZE];
		byte[] bitMapRecords = new byte[(int) (recordsPerBlock + 7) / 8];
		
		while (relationFileSize > (bitMapBlockNumber)*(DiskSpaceManager.BLOCK_SIZE)) {
			bufferManager.read(1, bitMapBlockNumber).get(blocksMap);
			for (long i = 1; i < blocksMap.length * 8; i++) {
				if ((blocksMap[(int) (blocksMap.length - i / 8 - 1)] & (1 << (i % 8))) > 0) {
					ByteBuffer currentBlock = bufferManager.read(1, i);
					currentBlock.get(bitMapRecords);
					for (int j = 0; j < bitMapRecords.length * 8; j++) {
						if ((bitMapRecords[bitMapRecords.length - j / 8 - 1] & (1 << (j % 8))) > 0) {
							byte[] blockEntry = new byte[ATTRIBUTE_RECORD_SIZE];
							currentBlock.get(blockEntry, j * ATTRIBUTE_RECORD_SIZE + (int)(recordsPerBlock + 7) / 8, ATTRIBUTE_RECORD_SIZE);
							tempAttribute = new Attribute(ByteBuffer.wrap(blockEntry));
							attributesList.add(tempAttribute);
						}
					}
				}
			}
			bitMapBlockNumber = bitMapBlockNumber + DiskSpaceManager.BLOCK_SIZE;
		}
		return recordEntry ;
	}

	
	/**
	 * Returns if there is another record in the Relation.
	 * @return If there is another record.
	 */
	public boolean hasNext() {
		int Blocktotal = relation.getFileSize()  / DiskSpaceManager.BLOCK_SIZE  ;
		if(currentBlock > Blocktotal -1){
			return false ;
		}else if(currentBlock == Blocktotal -1){
			if(nextRecord >= relation.getRecordsPerBlock()){
				return false ;
			}else{
				return true ;
			}
		}
	}
	
	
	public PhysicalAddress getAddress() {
		return BufferManager.getPhysicalAddress(relation.getRelationId(), currentBlock);
	}
	
	
}
