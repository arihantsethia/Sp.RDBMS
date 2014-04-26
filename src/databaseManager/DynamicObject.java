package databaseManager;

import java.nio.ByteBuffer;
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
			} else if (attributes.get(i).getType() == Attribute.Type.Float) {
				obj[i] = new Float(0.0);
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
			} else if (attributes.get(i).getType() == Attribute.Type.Float) {
				temp.obj[i] = serializedBuffer.getFloat();
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
						serializedBuffer.putChar(((String) temp.obj[i]).charAt(j));
					} else {
						serializedBuffer.putChar('\0');
					}
				}
			} else if (attributes.get(i).getType() == Attribute.Type.Float) {
				if (temp.obj[i] == null) {
					serializedBuffer.putFloat(Float.MIN_VALUE);
				} else {
					serializedBuffer.putFloat((Float) temp.obj[i]);
				}
			} else if (attributes.get(i).getType() == Attribute.Type.Int) {
				if (temp.obj[i] == null) {
					serializedBuffer.putInt(Integer.MIN_VALUE);
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
			} else if (values[i] instanceof Float) {
				obj[i] = (Float) values[i];
			}
		}
	}

	public void reset() {
		for (int i = 0; i < obj.length; i++) {
			if (obj[i] instanceof String) {
				obj[i] = (String) "";
			} else if (obj[i] instanceof Integer) {
				obj[i] = (Integer) Integer.MIN_VALUE;
			} else if (obj[i] instanceof Float) {
				obj[i] = (Float) Float.MIN_VALUE;
			}
		}
	}

	public void printRecords() {
		for (int i = 0; i < attributes.size(); i++) {
			if (attributes.get(i).getType() == Attribute.Type.Char) {
				System.out.printf("%-" + (attributes.get(i).getAttributeSize() + 1) / 2 + "s | ", (String) obj[i]);
			} else if (attributes.get(i).getType() == Attribute.Type.Int) {
				System.out.printf("%-10s | ", ((Integer) obj[i]).toString());
			} else if (attributes.get(i).getType() == Attribute.Type.Float) {
				System.out.printf("%-10s | ", ((Float) obj[i]).toString());
			}
		}
	}

	public void printRecords(String s) {
		for (int i = 0; i < attributes.size(); i++) {
			if (attributes.get(i).getType() == Attribute.Type.Char && attributes.get(i).getName().equals(s)) {
				System.out.printf("%-" + (attributes.get(i).getAttributeSize() + 1) / 2 + "s | ", (String) obj[i]);
			} else if (attributes.get(i).getType() == Attribute.Type.Int && attributes.get(i).getName().equals(s)) {
				System.out.printf("%-10s | ", ((Integer) obj[i]).toString());
			} else if (attributes.get(i).getType() == Attribute.Type.Float && attributes.get(i).getName().equals(s)) {
				System.out.printf("%-10s | ", ((Float) obj[i]).toString());
			}
		}
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
				if (((Integer) obj[i]).compareTo((Integer) newObj.obj[i])>0) {
					return 1;
				} else if (((Integer) obj[i]).compareTo((Integer) newObj.obj[i])<0) {
					return -1;
				}
			} else if (obj[i] instanceof Float) {
				if (((Float) obj[i]).compareTo((Float) newObj.obj[i])>0) {
					return 1;
				} else if (((Float) obj[i]).compareTo((Float) newObj.obj[i])<0) {
					return -1;
				}
			}
		}
		return 0;
	}

	@Override
	public boolean equals(Object o) {
		DynamicObject newObj = (DynamicObject) o;
		for (int i = 0; i < obj.length; i++) {
			if (obj[i] instanceof String) {
				if (!((String) obj[i]).equals((String) newObj.obj[i])) {
					return false;
				}
			} else if (obj[i] instanceof Integer) {
				if (!((Integer) obj[i]).equals((Integer) newObj.obj[i])) {
					return false;
				}
			} else if (obj[i] instanceof Float) {
				if (!((Float) obj[i]).equals((Float) newObj.obj[i])) {
					return false;
				}
			}
		}
		return true;
	}
}
