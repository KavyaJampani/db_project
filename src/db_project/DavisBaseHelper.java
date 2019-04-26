package db_project;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Map;


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

	public static void getColumnNames(String tableName) {

		try{
			RandomAccessFile table = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");

			int noOfPages = (int) (table.length() / pageSize);

			for (int i = 0; i < noOfPages; i++) {
				table.seek(pageSize * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {
					table.seek((pageSize * i) + 8);


				}
			}

		}
		catch(Exception e){
			e.printStackTrace();
		}

	}

	////havent finished, might not worked
	public static void exhaustiveSearch(RandomAccessFile table, String search){

		try{

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

				if(curTableName.equals(search) ){
					table.seek(8+(i*2));
					table.writeShort(-1);

					//decrement record count
					table.seek(1);
					recordCount = (byte) (recordCount - 1);
					table.writeByte(recordCount);

					break;
				}
				else {
					curTableName = "";
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}

	}


}

	