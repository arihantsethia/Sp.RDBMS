package databaseManager;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Vector;

public class DynamicObject implements Comparable<Object> {
	public Object[] obj;
	public Vector<Attribute> attributes;
	public int size;

	public DynamicObject() {

	}

	public DynamicObject(int n) {
		obj = new Object[n];
		for (int i = 0; i < n; i++) {
			obj[i] = null;
		}
	}

	public DynamicObject(Vector<Attribute> _attribtues) {
		attributes = _attribtues;
		size = 0;
		obj = new Object[attributes.size()];
		for (int i = 0; i < attributes.size(); i++) {
			if (attributes.get(i).getType() == Attribute.Type.Char) {
				obj[i] = new String();
			} else if (attributes.get(i).getType() == Attribute.Type.Int) {
				obj[i] = new Integer(0);
			}
			size += attributes.get(i).getAttributeSize();
		}
	}

	public DynamicObject deserialize(byte[] serialBytes) {
		ByteBuffer serializedBuffer = ByteBuffer.wrap(serialBytes);
		serializedBuffer.position(0);
		DynamicObject temp = new DynamicObject(attributes);
		for (int i = 0; i < attributes.size(); i++) {
			if (attributes.get(i).getType() == Attribute.Type.Char) {
				String str = "";
				for (int j = 0; j < attributes.get(i).getAttributeSize() / 2; j++) {
					char tempChar = serializedBuffer.getChar();
					if (tempChar != '\0') {
						str += tempChar;
					}
				}
				temp.obj[i] = str;
			} else {
				temp.obj[i] = serializedBuffer.getInt();
			}
		}
		return temp;
	}

	public ByteBuffer serialize(DynamicObject temp) {
		ByteBuffer serializedBuffer = ByteBuffer.allocate(size);
		serializedBuffer.position(0);
		for (int i = 0; i < attributes.size(); i++) {
			if (attributes.get(i).getType() == Attribute.Type.Char) {
				for (int j = 0; j < attributes.get(i).getAttributeSize() / 2; j++) {
					if (j < ((String) temp.obj[i]).length()) {
						serializedBuffer.putChar(((String) temp.obj[i]).charAt(j + 1));
					} else {
						serializedBuffer.putChar('\0');
					}
				}
			} else {
				if (temp.obj[i] == null) {
					serializedBuffer.putInt(0);
				} else {
					serializedBuffer.putInt((Integer) temp.obj[i]);
				}
			}
		}
		serializedBuffer.position(0);
		return serializedBuffer;
	}

	public void assignValues(Object[] values) {
		for (int i = 0; i < values.length; i++) {
			if (values[i] instanceof String) {
				obj[i] = (String) values[i];
			} else if (values[i] instanceof Integer) {
				obj[i] = (Integer) values[i];
			}
		}
	}

	public String printRecords() {
		String result="";
		for (int i = 0; i < attributes.size(); i++) {
			if (attributes.get(i).getType() == Attribute.Type.Char) {
				result = (String) obj[i] + " , " + result;
			} else {
				result = ((Integer) obj[i]).toString() + " , " + result;
			}
		}
		return result;
	}

	@Override
	public int compareTo(Object o) {
		DynamicObject newObj = (DynamicObject) o;
		for (int i = 0; i < obj.length; i++) {
			if (obj[i] instanceof String) {
				if (((String) obj[i]).compareTo((String) newObj.obj[i]) > 0) {
					return 1;
				} else if (((String) obj[i]).compareTo((String) newObj.obj[i]) < 0) {
					return -1;
				}
			} else if (obj[i] instanceof Integer) {
				if (((Integer) obj[i]) > (Integer) newObj.obj[i]) {
					return 1;
				} else if ((Integer) obj[i] < (Integer) newObj.obj[i]) {
					return -1;
				}
			}
		}
		return 0;
	}
}
