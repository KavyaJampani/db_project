package db_project;

import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.IOException;
import java.io.RandomAccessFile;
//import java.util.ArrayList;
//import java.util.LinkedHashMap;
//import java.util.Map;
//import java.util.Set;
//import java.util.TreeSet;

//import db_project.TablePage;
//import db_project.Tree;


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

			if ((filename+".tbl").equals(table))
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
			updateMetaColumns(tableName, columnNames);


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
			byte recordCount = davisbaseTablesCatalog.readByte();
			recordCount = (byte) (recordCount + 1);
			davisbaseTablesCatalog.seek(1);
			davisbaseTablesCatalog.writeByte(recordCount);

			//Update Array of Record Locations
			davisbaseTablesCatalog.seek(7+ ((recordCount-1)*2)-1);
			short oldStartLocation = davisbaseTablesCatalog.readShort();
			davisbaseTablesCatalog.seek(7+ ((recordCount-1)*2)+1);
			davisbaseTablesCatalog.writeShort(oldStartLocation - recordSpace+1);

			//Update Start of Content Location
			davisbaseTablesCatalog.seek(2);
			davisbaseTablesCatalog.writeShort(oldStartLocation - recordSpace+1);

			//get prev rowid
			davisbaseTablesCatalog.seek(oldStartLocation+3);
			int prevRowid = davisbaseTablesCatalog.readInt();

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

			davisbaseTablesCatalog.close();


		}
		catch(Exception e){
			e.printStackTrace();
		}

	}
	public static void updateMetaColumns(String tableName, String[] columnNames){
		try{

			RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");


			short[] offset = new short[columnNames.length];
			short[] payloadLengths = new short[columnNames.length];

			//Increment record count
			davisbaseColumnsCatalog.seek(1);
			byte recordCount = davisbaseColumnsCatalog.readByte();
			recordCount = (byte) (recordCount + columnNames.length);
			davisbaseColumnsCatalog.seek(1);
			davisbaseColumnsCatalog.writeByte(recordCount);

			// get previous start of content location
			davisbaseColumnsCatalog.seek(2);
			short oldStartLocation = davisbaseColumnsCatalog.readShort();

			// set the first element's info first so we can loop through the rest
			String[] columnInfo0 = columnNames[0].split(" ");

			int payloadLength0 = tableName.length() + columnInfo0[0].length() + columnInfo0[1].length();
			int recordHeaderLength0 = 1 + 3;
			int totalRecordLength0 = recordHeaderLength0 + payloadLength0;
			int recordSpace0 = 2 + 4 + totalRecordLength0;

			payloadLengths[0] = (short) recordSpace0;
			offset[0] = (short)(oldStartLocation - payloadLengths[0]+1);

			//the rest
			for(int i = 1; i< columnNames.length; i++){
				String[] columnInfo = columnNames[i].split(" ");

				int payloadLength = tableName.length() + columnInfo[0].length() + columnInfo[1].length();
				int recordHeaderLength = 1 + 3;
				int totalRecordLength = recordHeaderLength + payloadLength;
				int recordSpace = 2 + 4 + totalRecordLength;

				payloadLengths[i] = (short) recordSpace;

				offset[i] = (short)(offset[i-1] - payloadLengths[i]);
			}


			//Update Array of Record Locations
			davisbaseColumnsCatalog.seek(7+ ((recordCount-columnNames.length)*2)+1);
			for (int i = 0; i< columnNames.length; i++){
				davisbaseColumnsCatalog.writeShort(offset[i]);
			}

			//Update Start of Content Location
			davisbaseColumnsCatalog.seek(2);
			davisbaseColumnsCatalog.writeShort(offset[offset.length-1]);


			for (int i = 0; i< columnNames.length; i++){

				String[] columnInfo = columnNames[i].split(" ");

				//get prev rowid
				if (i==0)
					davisbaseColumnsCatalog.seek(oldStartLocation+2);
				else
					davisbaseColumnsCatalog.seek(offset[i-1]+2);
				int prevRowid = davisbaseColumnsCatalog.readInt();

				//add record value

				davisbaseColumnsCatalog.seek(offset[i]);

				//Record 1
				//davisbaseColumnsCatalog.seek(offset[0]);
				// Set Length of Payload
				davisbaseColumnsCatalog.writeShort(payloadLengths[i]);
				// Set rowid
				davisbaseColumnsCatalog.writeInt(prevRowid+1);
				// Set Number of Columns
				davisbaseColumnsCatalog.write(0x03);
				// Store Array of Column Data Types
				davisbaseColumnsCatalog.write(0x0C + tableName.length());
				davisbaseColumnsCatalog.write(0x0C + columnInfo[0].length());
				davisbaseColumnsCatalog.write(0x0C + columnInfo[1].length());
				// Store List of Column Data Values
				davisbaseColumnsCatalog.writeBytes(tableName);
				davisbaseColumnsCatalog.writeBytes(columnInfo[0]);
				davisbaseColumnsCatalog.writeBytes(columnInfo[1].toUpperCase());

			}

			davisbaseColumnsCatalog.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}

	}



	public static void dropTable(String tableName){

        try{
            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");

			//get record count
			davisbaseTablesCatalog.seek(1);
			byte recordCount = davisbaseTablesCatalog.readByte();

			short[] offset = new short[recordCount];

			//access array of record locations
			davisbaseTablesCatalog.seek(8);

			System.out.println(recordCount);
			for (int i = 0; i< recordCount; i++){
				offset[i] = davisbaseTablesCatalog.readShort();
			}
			String curTableName = "";
			byte curChar;
			char ch ;
			for (int i = 0; i< recordCount; i++){

				davisbaseTablesCatalog.seek(offset[i]+7);
				byte stringSize = davisbaseTablesCatalog.readByte();
				davisbaseTablesCatalog.seek(offset[i]+8);
				for (int j = 0; j< stringSize-12; j++){

					curChar = davisbaseTablesCatalog.readByte();
					ch = (char) curChar;
					curTableName = curTableName + ch;

				}
				//TODO: known bug: davisbase_columns isn't read
				//System.out.println(curTableName);
				if(curTableName.equals(tableName) ){
					davisbaseTablesCatalog.seek(8+(i*2));
					davisbaseTablesCatalog.writeShort(-1);

					//decrement record count
					davisbaseTablesCatalog.seek(1);
					recordCount = (byte) (recordCount - 1);
					davisbaseTablesCatalog.writeByte(recordCount);

					break;
				}
				else {
					curTableName = "";
				}

			}

            File file = new File("data/userdata/" + tableName +".tbl");
            file.delete();

        }
        catch(Exception e){
            e.printStackTrace();
        }

	}
	public static void parseQueryString(String queryString) {
		System.out.println("Parsing the string:\"" + queryString + "\"");
		String tableName;
		String[] columnNames;
		String[] condition = new String[0];
		String temp[] = queryString.split("where");
		tableName = temp[0].split("from")[1].trim();
		columnNames = temp[0].split("from")[0].replaceAll("select", " ").split(",");

		if (!findTable(tableName)) {
			System.out.println("Table not present");
		}

		else {
			for (int i = 0; i < columnNames.length; i++)
				columnNames[i] = columnNames[i].trim();

		if (temp.length > 1)
				condition = DavisBasePrompt.check(temp[1]);

		DavisBasePrompt.Query(tableName, columnNames, condition);
		}
	}
	
	
}

	