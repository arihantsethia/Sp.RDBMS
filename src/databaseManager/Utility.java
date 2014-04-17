package databaseManager;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import queriesManager.QueryParser;

import databaseManager.Attribute.Type;

public class Utility {
	private static Utility uty;

	public static Utility getUtility() {
		if (uty != null) {
			return uty;
		}
		uty = new Utility();
		return uty;
	}

	public static boolean isSameType(String type1, String Type2) {
		if (type1.equals("int")) {
			try {
				Integer.parseInt(Type2);
				return true;
			} catch (NumberFormatException e) {
				return false;
			}
		} else if (type1.equals("char")) {
			if (Type2.length() == 3 && Type2.charAt(0) == '\'' && Type2.charAt(2) == '\'') {
				return true;
			}
		} else if (type1.equals("float")) {

		}
		return false;
	}

	public int stringToInt(String s) {
		return Integer.parseInt(s);
	}

	public char stringToChar(String s) {
		return s.charAt(1);
	}

	public boolean isVarChar(String s, int size) {
		s = s.trim();
		if (s.length() >= 2 && s.length() <= size) {
			if (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'') {
				return true;
			}
			return false;
		}
		return false;
	}

	public static boolean isString(String s) {
		if (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'') {
			return true;
		} else {
			return false;
		}
	}

	public static boolean isCorrectkeywordType(String s, String size) {
		s = s.trim();
		if (s.indexOf("char(") == 0 && s.charAt(s.length() - 1) == ')') {
			try {
				Integer.parseInt(s.substring(5, s.length() - 1).trim());
				size = s.substring(5, s.length() - 1).trim();
			} catch (NumberFormatException e) {
				return false;
			}
			s = "char";
			return true;
		} else if (s.equals("int") || s.equals("float")) {
			size = "-1";
			return true;
		}
		return false;
	}

	public static ByteBuffer serialize(String[] columnList, String[] valueList, Vector<Attribute> attributesList, int recordSize) {
		ByteBuffer serializedBuffer = ByteBuffer.allocate(recordSize);
		if ((columnList.length == valueList.length) && (columnList.length <= attributesList.size())) {
			Map<String, Integer> columnMap = new HashMap<String, Integer>();
			for (int i = 0; i < columnList.length; i++) {
				if (!columnMap.containsKey(columnList[i].trim())) {
					columnMap.put(columnList[i].trim(), i);
					valueList[i] = valueList[i].trim();
				} else {
					System.out.println(" column name repeated");
					return null;
				}
			}
			for (int i = 0; i < attributesList.size(); i++) {
				if (columnMap.containsKey(attributesList.get(i).getAttributeName())) {
					int pos = columnMap.get(attributesList.get(i).getAttributeName());
					if (attributesList.get(i).getAttributeType() == Attribute.Type.Int) {
						if (Utility.getUtility().isSameType("int", valueList[pos])) {
							serializedBuffer.putInt(Utility.getUtility().stringToInt(valueList[pos]));
						} else {
							System.out.println(pos + "type mismatch : " + attributesList.get(i).getAttributeName());
							return null;
						}
					} else if (attributesList.get(i).getAttributeType() == Attribute.Type.Char) {
						if (Utility.getUtility().isVarChar(valueList[pos], attributesList.get(i).getAttributeSize())) {
							for (int j = 0; j < attributesList.get(i).getAttributeSize() / 2; j++) {
								if (j < valueList[pos].length() - 2) {
									serializedBuffer.putChar(valueList[pos].charAt(j + 1));
								} else {
									serializedBuffer.putChar('\0');
								}
							}
						} else {
							System.out.println("type mismatch : " + attributesList.get(i).getAttributeName());
							return null;
						}
					} else {
						System.out.println("type total mismatch : " + attributesList.get(i).getAttributeName());
						return null;
					}
				} else if (attributesList.get(i).isNullable()) {
					if (attributesList.get(i).getAttributeType() == Attribute.Type.Int) {
						serializedBuffer.position(serializedBuffer.position() + 4);
					} else if (attributesList.get(i).getAttributeType() == Attribute.Type.Char) {
						serializedBuffer.position(serializedBuffer.position() + attributesList.get(i).getAttributeSize());
					}
				} else {
					System.out.println(" value needed : " + attributesList.get(i).getAttributeName());
					return null;
				}
			}
		} else {
			System.out.println(" Length of column list and values list not same :(");
			return null;
		}
		serializedBuffer.position(0);
		return serializedBuffer;
	}

	public static String getRelationName(String s) {
		if (s.contains("as"))
			return s.substring(0, s.indexOf("as")).trim();
		else
			return s.substring(0, s.indexOf(".")).trim();
	}

	public static String getNickName(String s) {
		if (s.contains("as"))
			return s.substring(s.indexOf("as") + 2).trim();
		else
			return s.substring(s.indexOf("as") + 1).trim();
	}

	public static String getAlias(String s) {
		s = s.toUpperCase();
		return s.substring(0, s.indexOf(".")).trim();
	}

	public static String getFieldName(String s) {
		s = s.toUpperCase();
		return s.substring(s.indexOf(".") + 1).trim();
	}

	public static boolean checkType(String s1, String s2) {
		String field1 = "", field2 = "";
		String key1 = "", key2 = "";
		String relationName1 = "", relationName2 = "";

		key1 = getAlias(s1);
		key2 = getAlias(s2);
		field1 = getFieldName(s1);
		field2 = getFieldName(s2);
		relationName1 = QueryParser.tableMap.get(key1);
		relationName2 = QueryParser.tableMap.get(key2);

		long newRelationId1 = ObjectHolder.getObjectHolder().getRelationIdByRelationName(relationName1);
		long newRelationId2 = ObjectHolder.getObjectHolder().getRelationIdByRelationName(relationName2);

		if (newRelationId1 != -1 && newRelationId2 != -1) {
			Relation newRelation1 = (Relation) ObjectHolder.getObjectHolder().getObject(newRelationId1);
			Relation newRelation2 = (Relation) ObjectHolder.getObjectHolder().getObject(newRelationId2);

			if (newRelation1.attributeType(field1).equals(newRelation2.attributeType(field2))) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	public static boolean getDataType(String s) {
		String field = "";
		String key = "";
		String relationName = "";

		// String key1 = getAlias(s1);
		// String key2 = getAlias(s2);
		// String field1 = getFieldName(s1);
		// String field2 = getFieldName(s2);
		// String relationName1 = QueryParser.tableMap.get(key1);
		// String relationName2 = QueryParser.tableMap.get(key2);
		//
		// long newRelationId1 =
		// ObjectHolder.getObjectHolder().getRelationIdByRelationName(relationName1);
		// long newRelationId2 =
		// ObjectHolder.getObjectHolder().getRelationIdByRelationName(relationName2);
		//
		// if (newRelationId1 != -1 && newRelationId2 != -1) {
		// Relation newRelation1 = (Relation)
		// ObjectHolder.getObjectHolder().getObject(newRelationId1);
		// Relation newRelation2 = (Relation)
		// ObjectHolder.getObjectHolder().getObject(newRelationId2);
		//
		// if(newRelation1.attributeType(field1).equals(newRelation2.attributeType(field2))){
		// return true;
		// }
		// else{
		// return false;
		// }
		// }
		// else{
		// return false;
		// }

		return false;
	}

	public static boolean isNum(String s) {
		try {
			int num = Integer.parseInt(s);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}

	public static boolean isVariable(String s) {
		int dotIndex = s.indexOf(".");
		if (dotIndex != -1) {
			String key = "", field = "";
			String relationName = "";

			key = getAlias(s);
			if (QueryParser.tableMap.containsKey(key)) {
				field = getFieldName(s);
				relationName = QueryParser.tableMap.get(key);
				long newRelationId = ObjectHolder.getObjectHolder().getRelationIdByRelationName(relationName);
				if (newRelationId != -1) {
					Relation newRelation = (Relation) ObjectHolder.getObjectHolder().getObject(newRelationId);
					Vector<Attribute> attributes = newRelation.getAttributes();
					if (!attributes.contains(field)) {
						return false;
					}
				} else {
					return false;
				}
			} else {
				return false;
			}
			return true;
		} else {
			return false;
		}
	}
}