package db_project;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;





public class DavisBaseHelper {
	
	
	public void initialize() throws IOException{

		try {
			File dir = new File("data/catalog");
			if(dir != null)
				dir.mkdir();
			dir = new File("data/user_data");
			if(dir != null)
				dir.mkdir();
			
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			System.out.println("Unable to create data container directory");
			System.out.println(e);
		}
	}
	
	public static void createTable(RandomAccessFile table, String tableName, String[] columnNames) {
	
	
	
	}
	
	
}

	