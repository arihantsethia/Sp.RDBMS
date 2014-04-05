package databaseManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class Attribute {
	public enum Type {
		Int, Long, Boolean, Float, Double, Char, Undeclared;
		public static Type toType(byte value) {
	        return Type.values()[value];
	    }
		public static byte toByte(Type value) {
	        return value;
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
		serializedBuffer.put((byte) (nullable ? 1 : 0));
		//serializedBuffer.put(type);
		return serializedBuffer;
	}

	public Attribute deserialize(ByteBuffer data) {

		return null;
	}

}
