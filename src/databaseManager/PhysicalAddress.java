package databaseManager;

import java.util.Objects;

public class PhysicalAddress {
	public long id;
	public long pageNumber;
	public int pageOffset;

	public PhysicalAddress() {
		id = -1;
		pageNumber = -1;
		pageOffset = -1;
	}

	public PhysicalAddress(long _id, long _pageNumber) {
		id = _id;
		pageNumber = _pageNumber;
		pageOffset = -1;
	}

	public PhysicalAddress(long _id, long _pageNumber, int _pageOffset) {
		id = _id;
		pageNumber = _pageNumber;
		pageOffset = _pageOffset;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(id, pageNumber,pageOffset);
	}

	@Override
	public boolean equals(Object obj) {
		PhysicalAddress tempObj = (PhysicalAddress) obj;
		if (tempObj.id == id && tempObj.pageNumber == pageNumber && tempObj.pageOffset == pageOffset) {
			return true;
		}
		return false;
	}
}
