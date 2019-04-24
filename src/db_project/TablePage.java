package db_project;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class TablePage {

    public static int PAGESIZE = 512;// page size
    

	// variables
	private byte pageType;
	private short numOfRecords;
	private short startingCell;
	private int rightChild;
	private short[] recordLocations;

	

}
