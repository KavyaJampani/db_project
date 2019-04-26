package db_project;

import java.util.Arrays;

public class Page {

    public int pageNo;
    public byte pageType;
    public byte recordCount;
    public short startLocation;
    public int rightSibling;
    public short[] recordLocations;
    public Record[] records;

    public int getPageNo() {
        return pageNo;
    }

    public void setPageNo(int pageNo) {
        this.pageNo = pageNo;
    }

    public byte getPageType() {
        return pageType;
    }

    public void setPageType(byte pageType) {
        this.pageType = pageType;
    }

    public byte getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(byte recordCount) {
        this.recordCount = recordCount;
    }

    public short getStartLocation() {
        return startLocation;
    }

    public void setStartLocation(short startLocation) {
        this.startLocation = startLocation;
    }

    public int getRightSibling() {
        return rightSibling;
    }

    public void setRightSibling(int rightSibling) {
        this.rightSibling = rightSibling;
    }

    public short[] getRecordLocations() {
        return recordLocations;
    }

    public void setRecordLocations(short[] recordLocations) {
        this.recordLocations = recordLocations;
    }

    public Record[] getRecords() {
        return records;
    }

    public void setRecords(Record[] records) {
        this.records = records;
    }

    @Override
    public String toString() {
        return "Page{" +
                "pageNo=" + pageNo +
                ", pageType=" + pageType +
                ", recordCount=" + recordCount +
                ", startLocation=" + startLocation +
                ", rightSibling=" + rightSibling +
                ", recordLocations=" + Arrays.toString(recordLocations) +
                ", records=" + Arrays.toString(records) +
                '}';
    }
}
