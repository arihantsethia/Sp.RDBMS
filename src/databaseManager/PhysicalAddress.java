package databaseManager;

import java.util.Objects;

public class PhysicalAddress {
    public long id;
    public long offset;

    public PhysicalAddress() {

    }

    public PhysicalAddress(long _id, long _offset) {
	id = _id;
	offset = _offset;
    }

    @Override
    public int hashCode() {
	return Objects.hash(id, offset);
    }

    @Override
    public boolean equals(Object obj) {
	PhysicalAddress tempObj = (PhysicalAddress) obj;
	if (tempObj.id == id && tempObj.offset == offset) {
	    return true;
	}
	return false;
    }
}
