/**
 * 	@author Arihant , Arun and Vishavdeep
 *  Class SystemCatalogManager.java
 *  This class access methods of class BufferManager. and it's methods will be accessed by DatabaseManager class. 
 *   
 */

package databaseManager;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.StringTokenizer;

/**
 * @param ATTRIBUTE_CATALOG
 *            string containing value "attribute_catalog.cat" RELATION_CATALOG
 *            string containing value "relation_catalog.cat" bufferManager
 *            object of class named BufferManager. that object will be initiated
 *            when constructor of this class is called. attributeList list of
 *            attributes
 */

public class SystemCatalogManager {

	public static final String ATTRIBUTE_CATALOG = "attribute_catalog.db";
	public static final String RELATION_CATALOG = "relation_catalog.db";
	public static final int ATTRIBUTE_RECORD_SIZE = 126;
	public static final long ATTRIBUTE_CATALOG_ID = 1;
	public static final int RELATION_RECORD_SIZE = 144;
	public static final long RELATION_CATALOG_ID = 0;

	private BufferManager bufferManager;
	private ArrayList<Attribute> attributesList;
	private RelationHolder relationHolder;
	public long totalAttributesCount;
	public long totalRelationsCount;

	public SystemCatalogManager() {
		attributesList = new ArrayList<Attribute>();
		bufferManager = BufferManager.getBufferManager();
		relationHolder = RelationHolder.getRelationHolder();
		totalRelationsCount = 2;
		loadRelationCatalog();
		loadAttributeCatalog();
	}

	public void loadRelationCatalog() {
		Relation tableRelation = new Relation("relation_catalog",
				RELATION_CATALOG_ID);
		tableRelation.setRecordSize(RELATION_RECORD_SIZE);
		relationHolder.addRelation(tableRelation);
		long relationCatalogSize = tableRelation.getFileSize();
		long bitMapBlockNumber = 0;
		long recordsPerBlock = tableRelation.getRecordsPerBlock();
		byte[] bitMapBytes = new byte[(int) (recordsPerBlock + 7) / 8];
		BitSet bitMapRecords;
		Relation tempRelation;
		while (true) {
			for (long i = 1; i < DiskSpaceManager.BLOCK_SIZE * Byte.SIZE; i++) {
				if ((i + bitMapBlockNumber + 1) * DiskSpaceManager.BLOCK_SIZE <= relationCatalogSize) {
					ByteBuffer currentBlock = bufferManager.read(
							RELATION_CATALOG_ID, bitMapBlockNumber + i);
					currentBlock.get(bitMapBytes);
					bitMapRecords = BitSet.valueOf(bitMapBytes);
					for (int j = 0; j < bitMapRecords.length(); j++) {
						if (bitMapRecords.get(j)) {
							byte[] blockEntry = new byte[RELATION_RECORD_SIZE];
							currentBlock.get(blockEntry);
							tempRelation = new Relation(
									ByteBuffer.wrap(blockEntry));
							if (totalRelationsCount < tempRelation
									.getRelationId()) {
								totalRelationsCount = tempRelation
										.getRelationId() + 1;
							}
							relationHolder.addRelation(tempRelation);
						}
					}
				} else {
					return;
				}
			}
			bitMapBlockNumber = bitMapBlockNumber + DiskSpaceManager.BLOCK_SIZE
					* Byte.SIZE;
		}
	}

	public void loadAttributeCatalog() {
		Relation attributeRelation = new Relation("attribute_catalog",
				ATTRIBUTE_CATALOG_ID);
		attributeRelation.setRecordSize(ATTRIBUTE_RECORD_SIZE);
		relationHolder.addRelation(attributeRelation);
		long attributeCatalogSize = attributeRelation.getFileSize();
		long bitMapBlockNumber = 0;
		long recordsPerBlock = attributeRelation.getRecordsPerBlock();
		byte[] bitMapBytes = new byte[(int) (recordsPerBlock + 7) / 8];
		BitSet bitMapRecords;
		Attribute tempAttribute;
		while (true) {
			for (long i = 1; i < DiskSpaceManager.BLOCK_SIZE * Byte.SIZE; i++) {
				if ((i + bitMapBlockNumber + 1) * DiskSpaceManager.BLOCK_SIZE <= attributeCatalogSize) {
					ByteBuffer currentBlock = bufferManager.read(
							ATTRIBUTE_CATALOG_ID, bitMapBlockNumber + i);
					currentBlock.get(bitMapBytes);
					bitMapRecords = BitSet.valueOf(bitMapBytes);
					for (int j = 0; j < bitMapRecords.length(); j++) {
						if (bitMapRecords.get(j)) {
							byte[] blockEntry = new byte[ATTRIBUTE_RECORD_SIZE];
							currentBlock.get(blockEntry);
							tempAttribute = new Attribute(
									ByteBuffer.wrap(blockEntry));
							if (totalAttributesCount < tempAttribute.getId()) {
								totalAttributesCount = tempAttribute.getId() + 1;
							}
							attributesList.add(tempAttribute);
						}
					}
				} else {
					return;
				}
			}
			bitMapBlockNumber = bitMapBlockNumber + DiskSpaceManager.BLOCK_SIZE
					* Byte.SIZE;
		}
	}

	public void updateRelationCatalog(long relationId, ByteBuffer bufferStream) {

	}

	public boolean createTable(String relationStmt) {
		relationStmt = relationStmt.substring(
				relationStmt.toLowerCase().indexOf("table") + 5).trim();
		String relationName = relationStmt.substring(0,
				relationStmt.indexOf("(")).trim();
		Relation newRelation = new Relation(relationName, totalRelationsCount);
		if (relationHolder.addRelation(newRelation)) {
			addRelationToCatalog(newRelation);
			StringTokenizer tokens = new StringTokenizer(
					relationStmt.substring(relationStmt.indexOf("(") + 1,
							relationStmt.indexOf(")")), ",");
			while (tokens.hasMoreTokens()) {
				StringTokenizer attributeTokens = new StringTokenizer(tokens
						.nextToken().trim(), " ");
				if (attributeTokens.countTokens() < 2) {
					System.out
							.println("Name and type of attribute needs to be specified");
					return false;
				}
				String attributeName = attributeTokens.nextToken().trim();
				String _attributeType = attributeTokens.nextToken().trim();
				Attribute.Type attributeType = Attribute
						.stringToType(_attributeType);
				boolean nullable = true;
				int size = Attribute.Type.getSize(attributeType);
				if (attributeType == Attribute.Type.Char) {
					size = size
							* Integer.parseInt(_attributeType.substring(
									_attributeType.indexOf("(") + 1,
									_attributeType.indexOf(")")).trim());
				}
				if (attributeTokens.countTokens() > 1) {
					System.out
							.println("Only 3 properties per attribute i.e name , type , null/not_null are allowed");
					return false;
				} else if (attributeTokens.countTokens() == 1) {
					if (attributeTokens.nextToken().trim()
							.equalsIgnoreCase("not_null")) {
						nullable = false;
					} else {
						System.out
								.println("Only 3 properties per attribute i.e name , type , null/not_null are allowed");
						return false;
					}
				}
				Attribute newAttribute = new Attribute(attributeName,
						attributeType, totalAttributesCount,
						newRelation.getRelationId(), size, nullable);
				addAttributeToCatalog(newAttribute);
			}
			updateRelationCatalog(newRelation.getRelationId(),
					newRelation.serialize());
			System.out.println("Created new table " + relationName + " :-)");
		} else {
			System.out.println("Table " + relationName + " already exists!");
		}
		return true;
	}

	public void addAttributeToCatalog(Attribute newAttribute) {
		int attributeRecordsPerBlock = (int) (DiskSpaceManager.BLOCK_SIZE * 8 / (1 + 8 * ATTRIBUTE_RECORD_SIZE));
		long freeBlockNumber = bufferManager
				.getFreeBlockNumber(ATTRIBUTE_CATALOG_ID);
		int freeRecordOffset = bufferManager.getFreeRecordOffset(
				ATTRIBUTE_CATALOG_ID, freeBlockNumber,
				attributeRecordsPerBlock, ATTRIBUTE_RECORD_SIZE);
		bufferManager.write(ATTRIBUTE_CATALOG_ID, freeBlockNumber,
				freeRecordOffset, newAttribute.serialize());
		int recordNumber = (freeRecordOffset - (attributeRecordsPerBlock + 7) / 8)
				/ ATTRIBUTE_RECORD_SIZE;
		bufferManager.writeRecordBitmap(ATTRIBUTE_CATALOG_ID, freeBlockNumber,
				attributeRecordsPerBlock, recordNumber, true);
		attributesList.add(newAttribute);
		totalAttributesCount++;
	}

	public void addRelationToCatalog(Relation newRelation) {
		int relationRecordsPerBlock = (int) (DiskSpaceManager.BLOCK_SIZE * 8 / (1 + 8 * RELATION_RECORD_SIZE));
		long freeBlockNumber = bufferManager
				.getFreeBlockNumber(RELATION_CATALOG_ID);
		int freeRecordOffset = bufferManager.getFreeRecordOffset(
				RELATION_CATALOG_ID, freeBlockNumber, relationRecordsPerBlock,
				RELATION_RECORD_SIZE);
		bufferManager.write(RELATION_CATALOG_ID, freeBlockNumber,
				freeRecordOffset, newRelation.serialize());
		int recordNumber = (freeRecordOffset - (relationRecordsPerBlock + 7) / 8)
				/ RELATION_RECORD_SIZE;
		bufferManager.writeRecordBitmap(RELATION_CATALOG_ID, freeBlockNumber,
				relationRecordsPerBlock, recordNumber, true);
		totalRelationsCount++;
	}

	public void close() {
		bufferManager.flush();
	}
}
