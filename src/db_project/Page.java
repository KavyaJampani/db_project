package db_project;

import java.io.RandomAccessFile;
import java.util.Arrays;

public class Page {

    static int pageSize = 512;
    public int pageNo;
    public byte pageType;
    public byte recordCount;
    public short startLocation;
    public int rightSibling;
    public short[] recordLocations;
    public Record[] records;

    public void incrementRecordCount(RandomAccessFile table){
        try {
            byte pageStart = (byte) (pageSize * pageNo);
            recordCount += 1;

            table.seek(pageStart + 1);
            table.writeByte(recordCount);
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public void decrementRecordCount(RandomAccessFile table){
        try {
            byte pageStart = (byte) (pageSize * pageNo);
            recordCount -= 1;

            table.seek(pageStart + 1);
            table.writeByte(recordCount);
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public void addRecordLocation(RandomAccessFile table, short newRecordSpace){
        try {
            byte pageStart = (byte) (pageSize * pageNo);
            short newRecordLocation = (short) (startLocation - newRecordSpace);

            // update Array of Record Location
            table.seek(pageStart+ 7+ ((recordCount)*2)+1);
            table.writeShort(newRecordLocation);

            //Update Start of Content Location
            table.seek(2);
            table.writeShort(newRecordLocation);
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

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
