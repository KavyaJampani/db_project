package db_project;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;


public class DavisBaseHelper {

	static long pageSize = 512;

	
	public static void createTable(RandomAccessFile table, String tableName, ArrayList<String> columnNames) {

		try{
			table.setLength(pageSize);
			table.seek(0);
			table.write(0x0D);
			table.write(0x00);
			table.seek(4);
			table.writeInt(-1);

			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			davisbaseTablesCatalog.seek(1);
			byte curNumRecords = davisbaseTablesCatalog.readByte();
			curNumRecords = (byte) (curNumRecords + 1);
			davisbaseTablesCatalog.seek(1);
			System.out.println(curNumRecords);
			//TODO: figure out why it's not writing this byte
			table.writeByte(curNumRecords);

		}
		catch(Exception e){
			e.printStackTrace();
		}
	
	
	}
	
	
}

	