package databaseManager;

import java.util.HashMap;
import java.util.Map;

public class ObjectHolder {

	public Map<Long, Object> objects;
	private static ObjectHolder self;

	private ObjectHolder() {
		if (objects == null) {
			objects = new HashMap<Long, Object>();
		}
	}

	public synchronized static ObjectHolder getObjectHolder() {
		if (self == null) {
			self = new ObjectHolder();
		}
		return self;
	};

	public boolean addObject(Object object) {
		if (object instanceof Relation) {
			Relation rObject = (Relation) object;
			if (getRelationId(rObject.getName()) != -1) {
				return false;
			}
			objects.put(rObject.getId(), rObject);
		} else if (object instanceof Index) {
			Index iObject = (Index) object;
			if (getRelationId(iObject.getName()) != -1) {
				return false;
			}
			objects.put(iObject.getId(), iObject);
		}

		return true;
	}

	public boolean addObjectToRelation(Object object, boolean addToSize) {
		if (object instanceof Attribute) {
			Attribute aObject = (Attribute) object;
			Object toObject = objects.get(aObject.getParentId());
			if (toObject instanceof Relation) {
				Relation relation = (Relation) toObject;
				relation.addAttribute(aObject, addToSize);
			} else if (toObject instanceof Index) {
				Index index = (Index) toObject;
				index.addAttribute(aObject,addToSize);
			}

		} else if (object instanceof Index) {
			Index iObject = (Index) object;
			Relation relation = (Relation) objects.get(iObject.getParentId());
			relation.addIndex(iObject);
		}

		return true;
	}

	public Object getObject(final long objectId) {
		return objects.get(objectId);
	}

	public String getObjectFileName(final long objectId) {
		Object object = objects.get(objectId);
		if (object instanceof Relation) {
			Relation relation = (Relation) object;
			return relation.getFileName();
		} else if (object instanceof Index) {
			Index index = (Index) object;
			return index.getFileName();
		} else {
			return "";
		}
	}

	public long getRelationId(String name) {
		for (Map.Entry<Long, Object> entry : objects.entrySet()) {
			Object objectEntry = entry.getValue();
			if (objectEntry instanceof Relation) {
				Relation relationEntry = (Relation) objectEntry;
				if (relationEntry.getName().equalsIgnoreCase(name)) {
					return relationEntry.getId();
				}
			}
		}
		return -1;
	}
	

	public long getIndexId(String relationName, String indexName) {
		long relationId = getRelationId(relationName);
		if(relationId!=-1){
			for (Map.Entry<Long, Object> entry : objects.entrySet()) {
				Object objectEntry = entry.getValue();
				if (objectEntry instanceof Index) {
					Index indexEntry = (Index) objectEntry;
					if (indexEntry.getName().equalsIgnoreCase(indexName)) {
						if(indexEntry.getParentId()==relationId){
							return indexEntry.getId();
						}
					}
				}
			}
		}
		return -1;
	}

	public boolean removeObject(long id) {
		if (objects.containsKey(id)) {
			objects.remove(id);
			return true;
		}
		return false;
	}

	// Deprecated
	public long getNewId() {
		long id = 2;
		while (true) {
			if (!objects.containsKey(id)) {
				return id;
			}
			id++;
		}
	}
}
