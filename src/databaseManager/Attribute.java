package databaseManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class Attribute {
	public enum Type {
		Int, Long, Boolean, Float, Double, Char, Undeclared;
		public static Type toType(int value) {
			return Type.values()[value];
		}

		public static int toInt(Type value) {
			return value.ordinal();
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

	// private ArrayList values;

	public Attribute(String _attributeName, Type _type, long _id, long _parentId) {
		attributeName = _attributeName;
		type = _type;
		id = _id;
		parentId = _parentId;
		nullable = true;
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
		// values = new ArrayList();
	}

	public Attribute(ByteBuffer serializedBuffer) {
		for (int i = 0; i < 15; i++) {
			if (serializedBuffer.getChar(2 * i) != '\0') {
				attributeName += serializedBuffer.getChar(2 * i);
			}
		}
		serializedBuffer.position(30);
		id = serializedBuffer.getLong();
		parentId = serializedBuffer.getLong();
		size = serializedBuffer.getInt();
		type = Type.toType(serializedBuffer.getInt());
		nullable = serializedBuffer.get()!=0;
	}

	public static Type stringToType(final String _type) {
		if (_type.equalsIgnoreCase("INT")) {
			return Type.Int;
		} else if (_type.equalsIgnoreCase("CHAR")) {
			return Type.Char;
		} else if (_type.equalsIgnoreCase("DOUBLE")) {
			return Type.Double;
		} else if (_type.equalsIgnoreCase("FLOAT")) {
			return Type.Float;
		} else if (_type.equalsIgnoreCase("LONG")) {
			return Type.Long;
		} else {
			return Type.Undeclared;
		}
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
		return serializedBuffer;
	}

}
