package db_project;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;


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

	
	public static void createTable( String tableFileName, ArrayList<String> columnNames) {

		try{
			RandomAccessFile table = new RandomAccessFile("data/userdata/" + tableFileName, "rw");
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
			//TODO: figure out why it won't print the byte value
            davisbaseTablesCatalog.writeByte(curNumRecords);

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

	