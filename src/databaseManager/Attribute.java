package databaseManager;

import java.util.ArrayList;

public class Attribute {
	public enum Type {
		Int, Long, Boolean, Float, Double, Char, Undeclared
	};

	public static final int CHAR_SIZE = Character.SIZE / Byte.SIZE;
	public static final int FLOAT_SIZE = Float.SIZE / Byte.SIZE;
	public static final int DOUBLE_SIZE = Double.SIZE / Byte.SIZE;
	public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;
	public static final int LONG_SIZE = Long.SIZE / Byte.SIZE;

	private String attributeName;
	private long parentId;
	private long id;
	private Type type;
	private int size;
	private boolean nullable;
	private ArrayList values;

	public Attribute(String _attributeName, Type _type, int _id) {
		attributeName = _attributeName;
		type = _type;
		id = _id;
		values = new ArrayList();
	}

	public Attribute(String _attributeName, Type _type, int _id, int _size) {
		attributeName = _attributeName;
		type = _type;
		id = _id;
		size = _size;
		values = new ArrayList();
	}

	public Attribute(String _attributeName, Type _type, int _id, int _parentId,
			boolean _nullable, int _size) {
		attributeName = _attributeName;
		type = _type;
		id = _id;
		parentId = _parentId;
		nullable = _nullable;
		size = _size;
		values = new ArrayList();
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

	public boolean addToValueList(Object _val) {
		if (values.contains(_val)) {
			return false;
		} else {
			values.add(_val);
			return true;
		}
	}

	public String toString() {
		String str = attributeName + " " + type;
		if (type == Type.Char) {
			str += "(" + size + ")";
		}
		return str;
	}
}
