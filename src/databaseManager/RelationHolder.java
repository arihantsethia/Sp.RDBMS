package databaseManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RelationHolder {

	private Map<Long, Relation> relations;
	private static RelationHolder self;

	private RelationHolder() {
		if (relations == null) {
			relations = new HashMap<Long, Relation>();
		}
	}

	public synchronized static RelationHolder getRelationHolder() {
		if (self == null) {
			self = new RelationHolder();
		}
		return self;
	};

	public void addRelation(Relation relation) {
		relations.put(relation.getRelationId(), relation);
	}

	public Relation getRelation(final long relationId) {
		return relations.get(relationId);
	}

	public long getRelationIdByRelationName(String name) {
		for (Map.Entry<Long, Relation> entry : relations.entrySet()) {
			Relation relationEntry = entry.getValue();
			if (relationEntry.getRelationName().equalsIgnoreCase(name)) {
				return relationEntry.getRelationId();
			}
		}
		return -1;
	}
}
