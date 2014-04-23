package queriesManager;

import java.util.TreeMap;
import java.util.Vector;

import databaseManager.Attribute;
import databaseManager.DynamicObject;
import databaseManager.ObjectHolder;
import databaseManager.Relation;
import databaseManager.Utility;

public class Projection extends Operation {
	public static TreeMap<String, Integer> tableMap;
	public static int attributeCount;

	public Projection() {
		tableMap = new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER);
	}

	void printAllTableAttributes(Vector<String> tableList) {
		int length = 0;
		for (int i = 0; i < tableList.size(); i++) {
			Relation relation = (Relation) ObjectHolder.getObjectHolder().getObject(ObjectHolder.getObjectHolder().getRelationId(Utility.getRelationName(tableList.elementAt(i))));
			for (int j = 0; j < relation.getAttributesCount(); j++) {

				if (relation.getAttributes().get(j).getType() == Attribute.Type.Char) {
					System.out.printf("%-" + (relation.getAttributes().get(j).getAttributeSize() + 1) / 2 + "s | ", Utility.getNickName(tableList.elementAt(i)) + "."
							+ relation.getAttributes().get(j).getName());
					length += (relation.getAttributes().get(j).getAttributeSize() + 1) / 2 + 4;
				} else {
					System.out.printf("%-10s | ", Utility.getNickName(tableList.elementAt(i)) + "." + relation.getAttributes().get(j).getName());
					length += 10 + 1;
				}
			}
		}
		attributeCount += length;
	}

	void printAllTableAttributes(Vector<String> tableList, int index) {
		int length = 0;
		Relation relation = (Relation) ObjectHolder.getObjectHolder().getObject(ObjectHolder.getObjectHolder().getRelationId(Utility.getRelationName(tableList.elementAt(index))));
		for (int j = 0; j < relation.getAttributesCount(); j++) {
			if (relation.getAttributes().get(j).getType() == Attribute.Type.Char) {
				System.out.printf("%-" + (relation.getAttributes().get(j).getAttributeSize() + 1) / 2 + "s | ", Utility.getNickName(tableList.elementAt(index)) + "."
						+ relation.getAttributes().get(j).getName());
				length += (relation.getAttributes().get(j).getAttributeSize() + 1) / 2 + 4;
			} else {
				System.out.printf("%-10s | ", Utility.getNickName(tableList.elementAt(index)) + "." + relation.getAttributes().get(j).getName());
				length += 10 + 1;
			}
		}
		attributeCount += length;
	}

	void printAttribute(Vector<String> tableList, int index, String s) {
		int length = 0;
		Relation relation = (Relation) ObjectHolder.getObjectHolder().getObject(ObjectHolder.getObjectHolder().getRelationId(Utility.getRelationName(tableList.elementAt(index))));
		for (int j = 0; j < relation.getAttributesCount(); j++) {
			if (relation.getAttributes().get(j).getType() == Attribute.Type.Char && relation.getAttributes().get(j).getName().equals(s)) {
				System.out.printf("%-" + (relation.getAttributes().get(j).getAttributeSize() + 1) / 2 + "s | ", Utility.getNickName(tableList.elementAt(index)) + "."
						+ relation.getAttributes().get(j).getName());
				length += (relation.getAttributes().get(j).getAttributeSize() + 1) / 2 + 4;
			} else if (relation.getAttributes().get(j).getType() == Attribute.Type.Int && relation.getAttributes().get(j).getName().equals(s)) {
				System.out.printf("%-10s | ", Utility.getNickName(tableList.elementAt(index)) + "." + relation.getAttributes().get(j).getName());
				length += 10 + 1;
			}
		}
		attributeCount += length;
	}

	void printAllRecords(Vector<DynamicObject> recordObjects, Vector<String> tableList) {
		for (int i = 0; i < tableList.size(); i++) {
			recordObjects.get(i).printRecords();
		}
	}

	void printAllRecords(Vector<DynamicObject> recordObjects, int index) {
		recordObjects.get(index).printRecords();
	}

	void initializeMap(Vector<String> tableList) {
		tableMap.clear();
		for (int i = 0; i < tableList.size(); i++) {
			String nickName = Utility.getNickName(tableList.get(i).trim());
			tableMap.put(nickName, i);
		}
	}

	void printRecords(String projectionString, Vector<String> tableList, Vector<DynamicObject> recordObjects) {
		initializeMap(tableList);
		String[] tokens = projectionString.split(",");
		for (int i = 0; i < tokens.length; i++) {
			if (tokens[i].trim().equals("*")) {
				printAllRecords(recordObjects, tableList);
			} else if (tokens[i].trim().contains("*")) {
				String nickName = Utility.getRelationName(tokens[i].trim());
				int index = tableMap.get(nickName);
				recordObjects.get(index).printRecords();
			} else {
				String nickName = Utility.getRelationName(tokens[i].trim());
				int index = tableMap.get(nickName);
				recordObjects.get(index).printRecords(Utility.getNickName(tokens[i].trim()));
			}
		}
		System.out.println();
	}

	void printTableAttributes(String projectionString, Vector<String> tableList, Vector<DynamicObject> recordObjects) {
		attributeCount = 0;
		initializeMap(tableList);
		String[] tokens = projectionString.split(",");
		for (int i = 0; i < tokens.length; i++) {
			if (tokens[i].trim().equals("*")) {
				printAllTableAttributes(tableList);
			} else if (tokens[i].trim().contains("*")) {
				String nickName = Utility.getRelationName(tokens[i].trim());
				int index = tableMap.get(nickName);
				printAllTableAttributes(tableList, index);
			} else {
				String nickName = Utility.getRelationName(tokens[i].trim());
				int index = tableMap.get(nickName);
				printAttribute(tableList, index, Utility.getNickName(tokens[i].trim()));
			}
		}
		System.out.println();
	}
}
