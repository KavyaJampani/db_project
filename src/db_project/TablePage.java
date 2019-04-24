package db_project;

import java.io.IOException;
import java.io.RandomAccessFile;

public class TablePage {

    public static int PAGESIZE = 512;// page size

    //Structure of the 9-byte header
    private byte pageType;
    private short numOfRecords;
    private short startingCell;
    private int rightChild;
    private short[] recordLocations;



}
