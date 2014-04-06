package databaseManager;

import java.nio.ByteBuffer;

public class Attribute {
	public enum Type {
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
				return 1;
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
	private boolean nullable;
	private boolean distinctEntries;
	// private ArrayList values;

	public Attribute(String _attributeName, Type _type, long _id, long _parentId) {
		attributeName = _attributeName;
		type = _type;
		id = _id;
		parentId = _parentId;
		nullable = true;
		size = getSize();
		distinctEntries = false;
		// values = new ArrayList();
	}

	public Attribute(String _attributeName, Type _type, long _id,
			long _parentId, int _size) {
		attributeName = _attributeName;
		type = _type;
		id = _id;
		parentId = _parentId;
		size = _size;
		nullable = true;
		distinctEntries = false;
		// values = new ArrayList();
	}

	public Attribute(String _attributeName, Type _type, long _id,
			long _parentId, int _size, boolean _nullable) {
		attributeName = _attributeName;
		type = _type;
		id = _id;
		parentId = _parentId;
		nullable = _nullable;
		size = _size;
		distinctEntries = false;
		// values = new ArrayList();
	}

	public Attribute(ByteBuffer serializedBuffer) {
		attributeName = "";
		for (int i = 0; i < ATTRIBUTE_NAME_LENGTH; i++) {
			if (serializedBuffer.getChar(2 * i) != '\0') {
				attributeName += serializedBuffer.getChar(2 * i);
			}
		}
		serializedBuffer.position(ATTRIBUTE_NAME_LENGTH*2);
		id = serializedBuffer.getLong();
		parentId = serializedBuffer.getLong();
		size = serializedBuffer.getInt();
		type = Type.toType(serializedBuffer.getInt());
		nullable = serializedBuffer.get() != 0;
		distinctEntries = serializedBuffer.get() != 0;
	}

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
	
	public void setDistinct(boolean _distinctEntries){
		distinctEntries = _distinctEntries;
	}

	/*
	 * public boolean addToValueList(Object _val) { if (values.contains(_val)) {
	 * return false; } else { values.add(_val); return true; } }
	 */
	public String toString() {
		String str = attributeName + " " + type;
		if (type == Type.Char) {
			str += "(" + size + ")";
		}
		return str;
	}

	public ByteBuffer serialize() {
		ByteBuffer serializedBuffer = ByteBuffer
				.allocate((int) SystemCatalogManager.ATTRIBUTE_RECORD_SIZE);
		for (int i = 0; i < ATTRIBUTE_NAME_LENGTH; i++) {
			if (i < attributeName.length()) {
				serializedBuffer.putChar(attributeName.charAt(i));
			} else {
				serializedBuffer.putChar('\0');
			}
		}
		serializedBuffer.putLong(id);
		serializedBuffer.putLong(parentId);
		serializedBuffer.putInt(size);
		serializedBuffer.putInt(Type.toInt(type));
		serializedBuffer.put((byte) (nullable ? 1 : 0));
		serializedBuffer.put((byte) (distinctEntries ? 1 : 0));
		return serializedBuffer;
	}

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

	public int getAttributeSize() {
		return size;
	}

}
