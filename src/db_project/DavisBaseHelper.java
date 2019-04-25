package db_project;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import db_project.TablePage;
import db_project.Tree;


public class DavisBaseHelper {

	static long pageSize = 512;

	public static boolean findTable(String filename) {

		File catalog = new File("data/catalog/");
		String[] tablenames = catalog.list();
		for (String table : tablenames) {
			if (filename.equals(table))
				return true;

		}
		File userdata = new File("data/userdata/");
		String[] tables = userdata.list();
		for (String table : tables) {

			if (filename.equals(table))
				return true;
		}
		return false;
	}

	
	public static void createTable( RandomAccessFile table, String tableName, String[] columnNames) {

		try{
//			RandomAccessFile table = new RandomAccessFile("data/userdata/" + tableFileName, "rw");
			table.setLength(pageSize);
			table.seek(0);
			table.write(0x0D);
			table.write(0x00);
			table.seek(4);
			table.writeInt(-1);
			table.writeBytes("davisbase_tables");
			table.close();

			updateMetaTable(tableName);
			updateMetaColumns(columnNames);


		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	public static void updateNumOfRecords(RandomAccessFile page){
		try{

			page.seek(1);
			byte curNumRecords = page.readByte();
			curNumRecords = (byte) (curNumRecords + 1);
			page.seek(1);
			page.writeByte(curNumRecords);


		}
		catch(Exception e){
			e.printStackTrace();
		}

	}

	public static void updateMetaTable(String tableName){
		try{

			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");

			int payloadLength = tableName.length();
			int recordHeaderLength = 1 + 1;
			int totalRecordLength = recordHeaderLength + payloadLength;
			int recordSpace = 2 + 4 + totalRecordLength;

			//Increment record count
			davisbaseTablesCatalog.seek(1);
			byte curNumRecords = davisbaseTablesCatalog.readByte();
			curNumRecords = (byte) (curNumRecords + 1);
			davisbaseTablesCatalog.seek(1);
			davisbaseTablesCatalog.writeByte(curNumRecords);

			//Update Array of Record Locations
			davisbaseTablesCatalog.seek(7+ ((curNumRecords-1)*2)-1);
			short oldStartLocation = davisbaseTablesCatalog.readShort();
			davisbaseTablesCatalog.seek(7+ ((curNumRecords-1)*2)+1);
			davisbaseTablesCatalog.writeShort(oldStartLocation - recordSpace+1);

			//Update Start of Content Location
			davisbaseTablesCatalog.seek(2);
			davisbaseTablesCatalog.writeShort(oldStartLocation - recordSpace+1);

			//get prev rowid
			davisbaseTablesCatalog.seek(oldStartLocation+3);
			int prevRowid = davisbaseTablesCatalog.readInt();
			System.out.println(prevRowid);

			//add record value
			davisbaseTablesCatalog.seek(oldStartLocation - recordSpace+1);

			// Set Length of Payload
			davisbaseTablesCatalog.writeShort(totalRecordLength);
			// Set rowid
			davisbaseTablesCatalog.writeInt(prevRowid+1);
			// Set Number of Columns
			davisbaseTablesCatalog.write(0x01);
			// Store Array of Column Data Types
			davisbaseTablesCatalog.write(0x0C + tableName.length());
			// Store List of Column Data Values
			davisbaseTablesCatalog.writeBytes(tableName);



		}
		catch(Exception e){
			e.printStackTrace();
		}

	}
	public static void updateMetaColumns(String[] columnNames){
		try{

			RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			updateNumOfRecords(davisbaseColumnsCatalog);



		}
		catch(Exception e){
			e.printStackTrace();
		}

	}




	public static void dropTable(String tableFileName){

        try{
            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");

            File file = new File("data/userdata/" + tableFileName );
            file.delete();

        }
        catch(Exception e){
            e.printStackTrace();
        }

	}
	
	
}

	