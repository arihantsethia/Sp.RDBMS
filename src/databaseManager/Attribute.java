package databaseManager;

import java.nio.ByteBuffer;

/**
 * @author Arihant Sethia, Arun Bhati, Vishavdeep Mattu This class defines the
 *         member functions and properties of attributes of a relation.
 */
public class Attribute {

	/**
	 * @author arihant This is an enum of Type. This is used to define the data
	 *         type of attribute.
	 */
	public static enum Type {
		Int, Long, Boolean, Float, Double, Char, Undeclared;
		public static Type toType(int value) {
			return Type.values()[value];
		}

		public static int toInt(Type value) {
			return value.ordinal();
		}

		public static int getSize(Type _type) {
			if (_type == Attribute.Type.Int) {
				return INT_SIZE;
			} else if (_type == Attribute.Type.Char) {
				return CHAR_SIZE;
			} else if (_type == Attribute.Type.Boolean) {
				return CHAR_SIZE;
			} else if (_type == Attribute.Type.Long) {
				return LONG_SIZE;
			} else if (_type == Attribute.Type.Float) {
				return FLOAT_SIZE;
			} else if (_type == Attribute.Type.Double) {
				return DOUBLE_SIZE;
			} else
				return 0;
		}
		
		public static String toString(Type _type) {
			if (_type == Attribute.Type.Int) {
				return "int";
			} else if (_type == Attribute.Type.Char) {
				return "char";
			} else if (_type == Attribute.Type.Boolean) {
				return "boolean";
			} else if (_type == Attribute.Type.Long) {
				return "long";
			} else if (_type == Attribute.Type.Float) {
				return "float";
			} else if (_type == Attribute.Type.Double) {
				return "double";
			} else
				return "";
		}
	};

	public static final int CHAR_SIZE = Character.SIZE / Byte.SIZE;
	public static final int FLOAT_SIZE = Float.SIZE / Byte.SIZE;
	public static final int DOUBLE_SIZE = Double.SIZE / Byte.SIZE;
	public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;
	public static final int LONG_SIZE = Long.SIZE / Byte.SIZE;

	public static final int ATTRIBUTE_NAME_LENGTH = 50;

	private String attributeName;
	private long parentId;
	private long id;
	private Type type;
	private int size;
	private int position;
	private boolean nullable;
	private boolean distinctEntries;
	private boolean isPartPK;
	private long pageNumber;
	private int recordOffset;

	/**
	 * Constructor function to create an object of Attribute Class
	 * 
	 * @param _attributeName
	 *            : This is the name of the attribute
	 * @param _type
	 *            : This is the data type of the attribute
	 * @param _id
	 *            : This is the unique identifier of the attribute
	 * @param _parentId
	 *            : This is the identifier of the relation to which attribute
	 *            belongs
	 */
	public Attribute(String _attributeName, Type _type, long _id, long _parentId) {
		attributeName = _attributeName;
		type = _type;
		id = _id;
		parentId = _parentId;
		nullable = true;
		size = getSize();
		position = -1;
		distinctEntries = false;
		isPartPK = false;
	}

	/**
	 * Constructor function to create an object of Attribute Class
	 * 
	 * @param _attributeName
	 *            : This is the name of the attribute
	 * @param _type
	 *            : This is the data type of the attribute
	 * @param _id
	 *            : This is the unique identifier of the attribute
	 * @param _parentId
	 *            : This is the identifier of the relation to which attribute
	 *            belongs
	 * @param _size
	 *            : This is the length of the attribute(needed mostly for
	 *            attribute with data type as char.
	 */
	public Attribute(String _attributeName, Type _type, long _id, long _parentId, int _size) {
		attributeName = _attributeName;
		type = _type;
		id = _id;
		parentId = _parentId;
		size = _size;
		position = -1;
		nullable = true;
		distinctEntries = false;
		isPartPK = false;
	}

	/**
	 * Constructor function to create an object of Attribute Class
	 * 
	 * @param _attributeName
	 *            : This is the name of the attribute
	 * @param _type
	 *            : This is the data type of the attribute
	 * @param _id
	 *            : This is the unique identifier of the attribute
	 * @param _parentId
	 *            : This is the identifier of the relation to which attribute
	 *            belongs
	 * @param _size
	 *            : This is the length of the attribute(needed mostly for
	 *            attribute with data type as char.
	 * @param _nullable
	 *            : This defines if an attribute can take null values or not.
	 */
	public Attribute(String _attributeName, Type _type, long _id, long _parentId, int _size, boolean _nullable, boolean _distinct) {
		attributeName = _attributeName;
		type = _type;
		id = _id;
		parentId = _parentId;
		nullable = _nullable;
		size = _size;
		position = -1;
		distinctEntries = _distinct;
		isPartPK = false;
	}

	/**
	 * This constructor accepts a buffer and parses it to give an attribute
	 * object
	 * 
	 * @param serializedBuffer
	 *            : Buffer containing serialized data of an attribute object
	 */
	public Attribute(ByteBuffer serializedBuffer) {
		attributeName = "";
		for (int i = 0; i < ATTRIBUTE_NAME_LENGTH; i++) {
			if (serializedBuffer.getChar(2 * i) != '\0') {
				attributeName += serializedBuffer.getChar(2 * i);
			}
		}
		serializedBuffer.position(ATTRIBUTE_NAME_LENGTH * 2);
		pageNumber = serializedBuffer.getLong();
		recordOffset = serializedBuffer.getInt();
		id = serializedBuffer.getLong();
		parentId = serializedBuffer.getLong();
		size = serializedBuffer.getInt();
		position = serializedBuffer.getInt();
		type = Type.toType(serializedBuffer.getInt());
		nullable = serializedBuffer.get() != 0;
		distinctEntries = serializedBuffer.get() != 0;
		isPartPK = serializedBuffer.get() != 0;
	}

	/**
	 * This function parses the string to data type.
	 * 
	 * @param _type
	 *            : String to be parsed.
	 * @return : Data Type derived after parsing the input string.
	 */
	public static Type stringToType(final String _type) {
		Type returnType = Type.Undeclared;
		if (_type.toLowerCase().equalsIgnoreCase("int")) {
			returnType = Attribute.Type.Int;
		} else if (_type.toLowerCase().equalsIgnoreCase("long")) {
			returnType = Attribute.Type.Long;
		} else if (_type.toLowerCase().equalsIgnoreCase("bool")) {
			returnType = Attribute.Type.Boolean;
		} else if (_type.toLowerCase().toLowerCase().startsWith("char")) {
			returnType = Attribute.Type.Char;
		} else if (_type.toLowerCase().equalsIgnoreCase("float")) {
			returnType = Attribute.Type.Float;
		} else if (_type.toLowerCase().equalsIgnoreCase("double")) {
			returnType = Attribute.Type.Double;
		}
		return returnType;
	}

	/**
	 * This function sets the distinct property of the attribute.
	 * 
	 * @param _distinctEntries
	 *            : This defines if an attribute must take distinct values only.
	 */
	public void setDistinct(boolean _distinctEntries) {
		distinctEntries = _distinctEntries;
	}

	public void setPosition(int _position) {
		position = _position;
	}
	public void setId(long _newId) {
		id = _newId;
	}
	
	public void setParentId(long _newId) {
		parentId = _newId;
	}

	public String toString() {
		return attributeName;
	}
	
	public void partPK(boolean val) {
		isPartPK = val;
	}
	
	public boolean isPartPK() {
		return isPartPK;
	}

	/**
	 * This function serializes the attribute object.
	 * 
	 * @return : This returns the ByteBuffer containing serialized data of
	 *         attribute object.
	 */
	public ByteBuffer serialize() {
		ByteBuffer serializedBuffer = ByteBuffer.allocate((int) SystemCatalogManager.ATTRIBUTE_RECORD_SIZE);
		for (int i = 0; i < ATTRIBUTE_NAME_LENGTH; i++) {
			if (i < attributeName.length()) {
				serializedBuffer.putChar(attributeName.charAt(i));
			} else {
				serializedBuffer.putChar('\0');
			}
		}
		serializedBuffer.putLong(pageNumber);
		serializedBuffer.putInt(recordOffset);
		serializedBuffer.putLong(id);
		serializedBuffer.putLong(parentId);
		serializedBuffer.putInt(size);
		serializedBuffer.putInt(position);
		serializedBuffer.putInt(Type.toInt(type));
		serializedBuffer.put((byte) (nullable ? 1 : 0));
		serializedBuffer.put((byte) (distinctEntries ? 1 : 0));
		serializedBuffer.put((byte) (isPartPK ? 1 : 0));
		return serializedBuffer;
	}

	/**
	 * This function evaluates and returns size of the attribute.
	 * 
	 * @return : The size of attribute.
	 */
	private int getSize() {
		if (type == Attribute.Type.Int) {
			return INT_SIZE;
		} else if (type == Attribute.Type.Char) {
			return CHAR_SIZE * size;
		} else if (type == Attribute.Type.Long) {
			return LONG_SIZE;
		} else if (type == Attribute.Type.Float) {
			return FLOAT_SIZE;
		} else if (type == Attribute.Type.Double) {
			return DOUBLE_SIZE;
		} else
			return 0;
	}

	/**
	 * This function returns the attribute size as set in member variable size.
	 * 
	 * @return : attribute size as set in member variable size.
	 */
	public int getAttributeSize() {
		return size;
	}

	/**
	 * This function returns the attribute id.
	 * 
	 * @return : attribute id.
	 */
	public long getId() {
		return id;
	}

	public long getParentId() {
		return parentId;
	}

	public int getPosition() {
		return position;
	}

	public String getName() {
		return attributeName;
	}

	public Type getType() {
		return type;
	}

	public boolean isNullable() {
		return nullable;
	}
	
	public void setAddress(long _pageNumber, int _pageOffset){
		pageNumber = _pageNumber;
		recordOffset = _pageOffset;
	}
	
	public long getPageNumber(){
		return pageNumber;
	}
	
	public int getRecordOffset(){
		return recordOffset;
	}

	public boolean isDistinct() {
		return distinctEntries;
	}
}
