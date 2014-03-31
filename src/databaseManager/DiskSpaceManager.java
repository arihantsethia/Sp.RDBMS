package databaseManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class DiskSpaceManager {

	/**
	 * This is the size of the block in bytes which will be taken into and
	 * returned from this class.
	 */
	public static final long BLOCK_SIZE = 4096;

	/**
	 * Creates a new instance of StorageManager with nothing inside of it.
	 */
	public DiskSpaceManager() {

	}

	/**
	 * Function call to open file give by filename if it exists else create a
	 * new file.
	 * 
	 * @param fileName
	 *            : name of the file to be opened
	 * @return FileChannel corresponding to the opened file
	 */

	public FileChannel openFile(final String fileName) {
		RandomAccessFile file;
		FileChannel fileChannel = null;
		try {
			file = new RandomAccessFile(fileName, "rw");
			fileChannel = file.getChannel();
		} catch (FileNotFoundException error) {
			// TODO Auto-generated catch block
			try {
				File newFile = new File(fileName);
				if (newFile.createNewFile()) {
					return openFile(fileName);
				} else {
					System.out.println("File Creation Unsuccesful.");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("Couldn't Create File : " + fileName);
				e.printStackTrace();
				System.exit(1);
			}
		}
		return fileChannel;
	}

	public static boolean closeFile(final FileChannel fileChannel) {
		try {
			fileChannel.close();
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Couldn't close the required file.");
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * This function reads the required block from the file
	 * 
	 * @param fileChannel
	 *            : FileChannel corresponding to file from which data has to be
	 *            read.
	 * @param block
	 *            : block number of the corresponding block
	 * @return : returns the bytebuffer of the read data.
	 */
	public ByteBuffer read(final FileChannel fileChannel,
			final long block) {
		MappedByteBuffer buffer = null;
		try {
			if (isValidBlockNumber(fileChannel, block)) {
				buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, block
						* BLOCK_SIZE, BLOCK_SIZE);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Couldn't retrieve data from required file");
			e.printStackTrace();
			System.exit(1);
		}
		return buffer;
	}

	public ByteBuffer read(final String fileName, final long block) {
		FileChannel fileChannel = openFile(fileName);
		return read(fileChannel, block);
	}

	public long write(final FileChannel fileChannel, final long block,
			final ByteBuffer writeBuffer) {
		try {
			if (isValidBlockNumber(fileChannel, block)) {
				return fileChannel.write(writeBuffer, block * BLOCK_SIZE);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Couldn't write to the required file");
			e.printStackTrace();
			System.exit(1);
		}
		return 0;
	}

	public long write(final String fileName, final long block,
			final ByteBuffer writeBuffer) {
		FileChannel fileChannel = openFile(fileName);
		return write(fileChannel, block, writeBuffer);
	}

	public boolean isValidBlockNumber(final FileChannel fileChannel,
			final long block) {
		try {
			if (fileChannel.size() <= (block + 1) * BLOCK_SIZE) {
				return true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		return false;
	}
}
