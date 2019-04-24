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
			table.close();

			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			int num_pages = (int) (davisbaseTablesCatalog.length() / pageSize);
			int page = 0;
			

			Map<Integer, TablePage> records= new LinkedHashMap<Integer, TablePage>();
			for (int i = 0; i < num_pages; i++) {
				davisbaseTablesCatalog.seek((i * pageSize) + 4);
				int filePointer = davisbaseTablesCatalog.readInt();
				if (filePointer == -1) {
					page = i;
					davisbaseTablesCatalog.seek(i * pageSize + 1);
					int num_records = davisbaseTablesCatalog.readByte();
					short[] rec_locations = new short[num_records];
					davisbaseTablesCatalog.seek((pageSize * i) + 8);
					for (int location = 0; location < num_records; location++) {
						rec_locations[location] = davisbaseTablesCatalog.readShort();
					}
					records = TablePage.retrievePayload(davisbaseTablesCatalog, rec_locations, i);
				}
			}
			davisbaseTablesCatalog.close();
			Set<Integer> rowIds = records.keySet();
			Set<Integer> sortedRowIds = new TreeSet<Integer>(rowIds);
			Integer rows[] = sortedRowIds.toArray(new Integer[sortedRowIds.size()]);
			int key = rows[rows.length - 1] + 1;

//			String[] values = { String.valueOf(key), tableFileName.trim(), "8", "10" };
//			insert("davisbase_tables", values);
			
			
			
			

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
	
//	public static void insert(String tableName, String[] values) {
//		try {
//			tableName = tableName.trim();
//			String path = "data/userdata/" + tableName + ".tbl";
//			if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
//				path = "data/catalog/" + tableName + ".tbl";
//
//			RandomAccessFile table = new RandomAccessFile(path, "rw");
//
//			String condition[] = { "table_name", "=", tableName };
//			String columnNames[] = { "*" };
//			Map<Integer, TablePage> column = TablePage.getcolumn(tableName, columnNames, condition);
//			String[] dataType = TablePage.getDataType(column);
//
//			int count = 0;
//			String[] nullable = new String[column.size()];
//			for (Map.Entry<Integer, PageNav> entry : column.entrySet()) {
//
//				PageNav PageNav = entry.getValue();
//				PageNav payload = PageNav.getPayload();
//				String[] data = payload.data;
//				nullable[count] = data[4];
//				count++;
//			}
//
//			String[] isNullable = nullable;
//
//			for (int i = 0; i < values.length; i++) {
//				if (values[i].equalsIgnoreCase("null") && isNullable[i].equals("NO")) {
//					System.out.println("Cannot insert NULL values in NOT NULL field");
//					table.close();
//					return;
//				}
//			}
//			condition = new String[0];
//
//			int pageNo = PageNav.getPage(tableName, Integer.parseInt(values[0]));
//
//			Map<Integer, PageNav> data = PageNav.getData(tableName, columnNames, condition);
//			if (data.containsKey(Integer.parseInt(values[0]))) {
//				System.out.println("Duplicate value for primary key");
//				table.close();
//				return;
//			}
//
//			byte[] payloadType = new byte[dataType.length - 1];
//			int payLoadSize = PageNav.getPayloadSize(tableName, values, payloadType, dataType);
//			payLoadSize = payLoadSize + 6;
//
//			int address = TreeFunctions.checkOverFlow(table, pageNo, payLoadSize);
//
//			if (address != -1) {
//				PageNav Builder1 = PageNav.AddPage(pageNo, Integer.parseInt(values[0]), (short) payLoadSize, payloadType,
//						values);
//				PageNav.payload(table, Builder1, address);
//			} else {
//				TreeFunctions.splitLeaf(table, pageNo);
//				int pNo = PageNav.getPage(tableName, Integer.parseInt(values[0]));
//				int addr = TreeFunctions.checkOverFlow(table, pNo, payLoadSize);
//				PageNav Builder1 = PageNav.AddPage(pNo, Integer.parseInt(values[0]), (short) payLoadSize, payloadType,
//						values);
//				PageNav.payload(table, Builder1, addr);
//			}
//			table.close();
//			
//		} catch (Exception e) {
//			
//			e.printStackTrace();
//		}
//	}
//	

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

	