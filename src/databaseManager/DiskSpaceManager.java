package databaseManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DiskSpaceManager {

	/**
	 * This is the size of the page in bytes which will be taken into and
	 * returned from this class.
	 */
	public static final long PAGE_SIZE = 4096;

	/**
	 * Creates a new instance of StorageManager with nothing inside of it.
	 */
	public DiskSpaceManager() {

	}

	/**
	 * Function call to open file given by filename if it exists else create a
	 * new file.
	 * 
	 * @param fileName
	 *            : name of the file to be opened.
	 * @return FileChannel: FileChannel of the corresponding file.
	 */

	public FileChannel openFile(final String fileName) {
		RandomAccessFile file;
		FileChannel fileChannel = null;
		System.out.println("DS:"+System.getProperty("user.dir"));
		try {
			file = new RandomAccessFile(System.getProperty("user.dir")+"/"+fileName, "rw");
			fileChannel = file.getChannel();
		} catch (FileNotFoundException error) {
			try {
				File newFile = new File(fileName);
				if (newFile.createNewFile()) {
					return openFile(fileName);
				} else {
					System.out.println("File Creation Unsuccesful.");
				}
			} catch (IOException e) {
				System.out.println("Couldn't Create File : " + fileName);
				e.printStackTrace();
				System.exit(1);
			}
		}
		return fileChannel;
	}

	/**
	 * Function call to close the fileChannel.
	 * 
	 * @param fileChannel
	 *            : FileChannel of the file to be closed.
	 * @return FileChannel: Whether the fileChannel was closed or not.
	 */
	public static boolean closeFile(final FileChannel fileChannel) {
		try {
			fileChannel.close();
			return true;
		} catch (IOException e) {
			System.out.println("Couldn't close the required file.");
			e.printStackTrace();
		}
		return false;
	}

	public boolean deleteFile(String fileName) {
		File file = new File(fileName);
		return file.delete();
	}

	/**
	 * This function reads the required page from the file
	 * 
	 * @param fileChannel
	 *            : FileChannel corresponding to file from which data has to be
	 *            read.
	 * @param page
	 *            : page number of the corresponding page
	 * @return : returns the byte buffer of the read data.
	 */
	public ByteBuffer read(final FileChannel fileChannel, final long page) {
		ByteBuffer buffer = ByteBuffer.allocate((int) PAGE_SIZE);
		try {
			if (isValidPageNumber(fileChannel, page)) {
				fileChannel.read(buffer, page * PAGE_SIZE);
			}
		} catch (IOException e) {
			System.out.println("Couldn't retrieve data from required file");
			e.printStackTrace();
		}
		return buffer;
	}

	public ByteBuffer read(final String fileName, final long page) {
		FileChannel fileChannel = openFile(fileName);
		return read(fileChannel, page);
	}

	public long write(final FileChannel fileChannel, final long page, final ByteBuffer writeBuffer) {
		try {
			if (isValidPageNumber(fileChannel, page)) {
				return fileChannel.write(writeBuffer, page * PAGE_SIZE);
			}
		} catch (IOException e) {
			System.out.println("Couldn't write to the required file");
			e.printStackTrace();
			System.exit(1);
		}
		return 0;
	}

	public long write(final String fileName, final long page, final ByteBuffer writeBuffer) {
		FileChannel fileChannel = openFile(fileName);
		return write(fileChannel, page, writeBuffer);
	}

	public boolean isValidPageNumber(final FileChannel fileChannel, final long page) {
		try {
			if (fileChannel.size() >= (page + 1) * PAGE_SIZE) {
				return true;
			} else if (fileChannel.size() / PAGE_SIZE == page) {
				fileChannel.write(getEmptyPage(), page * PAGE_SIZE);
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		return false;
	}

	public static ByteBuffer getEmptyPage() {
		return ByteBuffer.allocate((int) PAGE_SIZE);
	}
}
