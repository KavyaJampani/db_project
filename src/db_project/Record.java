package db_project;


public class Record {

    public int pageNumber;
    public short payLoadSize;
    public int rowId;
    public byte[] colDataTypes;
    public String[] data;


    public int getPageNumber() {
        return pageNumber;
    }

    public void setPageNumber(int pageNumber) {
        this.pageNumber = pageNumber;
    }

    public short getPayLoadSize() {
        return payLoadSize;
    }

    public void setPayLoadSize(short payLoadSize) {
        this.payLoadSize = payLoadSize;
    }

    public int getRowId() {
        return rowId;
    }

    public void setRowId(int rowId) {
        this.rowId = rowId;
    }

    public byte[] getColDataTypes() {
        return colDataTypes;
    }

    public void setColDataTypes(byte[] colDataTypes) {
        this.colDataTypes = colDataTypes;
    }

    public String[] getData() {
        return data;
    }

    public void setData(String[] data) {
        this.data = data;
    }
}
