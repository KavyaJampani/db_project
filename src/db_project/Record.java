package db_project;

import java.util.Arrays;

public class Record {

    public int pageNo;
    public short location;
    public short payLoadSize;
    public int rowId;
    public byte columnCount;
    public byte[] colDataTypes;
    public String[] data;

    public String displayRow() {

        String displayString = "\t" + rowId ;
        for (String colVal : data)
        {
            displayString += "\t" + colVal;
        }
        return  displayString;
    }

    public Record(short location) {
        this.location = -1;
    }
    public Record() {
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

    public byte getColumnCount() {
        return columnCount;
    }

    public void setColumnCount(byte columnCount) {
        columnCount = columnCount;
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

    @Override
    public String toString() {
        return "Record{" +
                "pageNo=" + pageNo +
                ", location=" + location +
                ", payLoadSize=" + payLoadSize +
                ", rowId=" + rowId +
                ", columnCount=" + columnCount +
                ", colDataTypes=" + Arrays.toString(colDataTypes) +
                ", data=" + Arrays.toString(data) +
                '}';
    }


}
