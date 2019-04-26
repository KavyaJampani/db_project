package db_project;

import java.io.File;
import java.io.RandomAccessFile;


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
			table.setLength(pageSize);
			table.seek(0);
			table.write(0x0D);
			table.write(0x00);
			table.seek(4);
			table.writeInt(-1);
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

			String[] insertValues = {tableName};
			insertRecord(davisbaseTablesCatalog, insertValues);

			davisbaseTablesCatalog.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}

	}

	public static void updateMetaColumns(String tableName, String[] columnNames){
		try{

			RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");

			for (int i = 0; i< columnNames.length; i++) {

				String[] columnInfo = columnNames[i].split(" ");
				String[] insertValues = {tableName, columnInfo[0], columnInfo[1].toUpperCase()};
				insertRecord(davisbaseColumnsCatalog, insertValues);
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
			davisbaseTablesCatalog.close();

        }
        catch(Exception e){
            e.printStackTrace();
        }

	}

	public static void showTables() {
		try{
			RandomAccessFile table = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");

			//get record count
			table.seek(1);
			byte recordCount = table.readByte();

			short[] offset = new short[recordCount];

			//access array of record locations
			table.seek(8);

			for (int i = 0; i< recordCount; i++){
				offset[i] = table.readShort();
			}
			String curTableName = "";
			byte curChar;
			char ch ;
			for (int i = 0; i< recordCount; i++){

				table.seek(offset[i]+7);
				byte stringSize = table.readByte();
				table.seek(offset[i]+8);
				for (int j = 0; j< stringSize-12; j++){

					curChar = table.readByte();
					ch = (char) curChar;
					curTableName = curTableName + ch;

				}
				System.out.println(curTableName);


				curTableName = "";

			}

			table.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}


	}

	public static void insertRecord(RandomAccessFile table, String[] insertValues) {

		try{

			int payloadLength = 0;

			for (int i = 0; i < insertValues.length; i++)
				payloadLength += insertValues[i].length() ;
			int recordHeaderLength = 1 + insertValues.length;
			int totalRecordLength = recordHeaderLength + payloadLength;
			int recordSpace = 2 + 4 + totalRecordLength;

			//Increment record count
			table.seek(1);
			byte recordCount = table.readByte();
			recordCount = (byte) (recordCount + 1);
			table.seek(1);
			table.writeByte(recordCount);

			short oldStartLocation;
			int prevRowid;
			if(recordCount-1 == 0){

				oldStartLocation = (short) pageSize;
				prevRowid = 0;

			}
			else{

				//get prev record location
				table.seek(2);
				oldStartLocation = table.readShort();

				//get prev rowid
				table.seek(oldStartLocation+2);
				prevRowid = table.readInt();

			}

			// update Array of Record Location
			table.seek(7+ ((recordCount-1)*2)+1);
			table.writeShort(oldStartLocation - recordSpace);

			//Update Start of Content Location
			table.seek(2);
			table.writeShort(oldStartLocation - recordSpace);


			//add record value
			table.seek(oldStartLocation - recordSpace);

			// Set Length of Payload
			table.writeShort(totalRecordLength);
			// Set rowid
			table.writeInt(prevRowid+1);
			// Set Number of Columns
			table.write(insertValues.length);

			for (int i = 0; i < insertValues.length; i++)
				// Store Array of Column Data Types
				table.write(0x0C + insertValues[i].length());
			for (int i = 0; i < insertValues.length; i++)
				// Store List of Column Data Values
				table.writeBytes(insertValues[i]);


		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

}

	