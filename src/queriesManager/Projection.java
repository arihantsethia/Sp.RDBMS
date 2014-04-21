package queriesManager;

import java.util.Vector;

import databaseManager.Attribute;
import databaseManager.ObjectHolder;
import databaseManager.Relation;
import databaseManager.Utility;

public class Projection extends Operation {
	public Projection() {

	}

	void printAllTableAttributes(Vector<String> tableList) {
		int length  = 0 ;
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
		System.out.println();
		while (length != 0) {
			System.out.print("-");
			length--;
		}
		System.out.println();
	}
}
