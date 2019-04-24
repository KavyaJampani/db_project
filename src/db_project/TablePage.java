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
	public byte noOfCols;
	public byte[] dataType;
	public String[] data;
	public int pageNumber;
	public short payLoadSize;
	public int rowId;
	public TablePage payload;
	public short location;
	public int pageNo;
	public byte pageType;
	public Map<Integer, TablePage> tuples;
    //Structure of the 9-byte header
//    private byte pageType;
//    private short numOfRecords;
//    private short startingCell;
//    private int rightChild;
//    private short[] recordLocations;
	public void setData(String[] data) {
		this.data = data;
	}

	public void setPayload(TablePage payload) {
		this.payload = payload;
	}
	
	public TablePage getPayload() {
		return payload;
	}
	
	public static Map<Integer, TablePage> retrievePayload(RandomAccessFile davisbaseTablesCatalog, short[] rec_locations,int pageNo) {
		Map<Integer, TablePage> table_pages = new LinkedHashMap<Integer, TablePage>();
		for (int position = 0; position < rec_locations.length; position++) {
			try {
				TablePage Page = new TablePage();
				Page.pageNumber = pageNo;

				Page.location = rec_locations[position];

				davisbaseTablesCatalog.seek(rec_locations[position]);

				short payLoadSize = davisbaseTablesCatalog.readShort();
				Page.payLoadSize = payLoadSize;

				int rowId = davisbaseTablesCatalog.readInt();
				Page.rowId = rowId;

				TablePage payload = new TablePage();
				byte num_cols = davisbaseTablesCatalog.readByte();
				payload.noOfCols = num_cols;

				byte[] dataType = new byte[num_cols];
				int colsRead = davisbaseTablesCatalog.read(dataType);
				payload.dataType = dataType;

				String data[] = new String[num_cols];
				payload.setData(data);

				for (int i = 0; i < num_cols; i++) {
					switch (dataType[i]) {
					case 0x00:
						data[i] = Integer.toString(davisbaseTablesCatalog.readByte());
						data[i] = "null";
						break;

					case 0x01:
						data[i] = Integer.toString(davisbaseTablesCatalog.readShort());
						data[i] = "null";
						break;

					case 0x02:
						data[i] = Integer.toString(davisbaseTablesCatalog.readInt());
						data[i] = "null";
						break;

					case 0x03:
						data[i] = Long.toString(davisbaseTablesCatalog.readLong());
						data[i] = "null";
						break;

					case 0x04:
						data[i] = Integer.toString(davisbaseTablesCatalog.readByte());
						break;

					case 0x05:
						data[i] = Integer.toString(davisbaseTablesCatalog.readShort());
						break;

					case 0x06:
						data[i] = Integer.toString(davisbaseTablesCatalog.readInt());
						break;

					case 0x07:
						data[i] = Long.toString(davisbaseTablesCatalog.readLong());
						break;

					case 0x08:
						data[i] = String.valueOf(davisbaseTablesCatalog.readFloat());
						break;

					case 0x09:
						data[i] = String.valueOf(davisbaseTablesCatalog.readDouble());
						break;

					case 0x0A:
						long tmp = davisbaseTablesCatalog.readLong();
						Date dateTime = new Date(tmp);
						break;

					case 0x0B:
						long tmp1 = davisbaseTablesCatalog.readLong();
						Date date = new Date(tmp1);
						break;

					default:
						int len = new Integer(dataType[i] - 0x0C);
						byte[] bytes = new byte[len];
						for (int j = 0; j < len; j++)
							bytes[j] = davisbaseTablesCatalog.readByte();
						data[i] = new String(bytes);
						break;
					}

				}

				Page.setPayload(payload);
				table_pages.put(rowId, Page);

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		return table_pages;
		
	}
	
//	public static Map<Integer, TablePage> getcolumn(String tableName, String[] columnNames, String[] condition) {
//
//		try {
//
//			RandomAccessFile table = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
//			int noOfPages = (int) (table.length() / PAGESIZE);
//
//			Map<Integer, String> colNames = getColumnNames("davisbase_columns");
//			Map<Integer, TablePage> records = new LinkedHashMap<Integer, TablePage>();
//			for (int i = 0; i < noOfPages; i++) {
//				table.seek(PAGESIZE * i);
//				byte pageType = table.readByte();
//				if (pageType == 0x0D) {
//
//					int noOfBuilders = table.readByte();
//					short[] BuilderLocations = new short[noOfBuilders];
//					table.seek((PAGESIZE * i) + 8);
//					for (int location = 0; location < noOfBuilders; location++) {
//						BuilderLocations[location] = table.readShort();
//					}
//					Map<Integer, TablePage> recordBuilders = new LinkedHashMap<Integer, TablePage>();
//					recordBuilders = retrievePayload(table, BuilderLocations, i);
//					records.putAll(recordBuilders);
//				}
//			}
//
//			if (condition.length > 0) {
//				Map<Integer, TablePage> filteredRecords = filterTuples(colNames, records, columnNames, condition);
//				table.close();
//				return filteredRecords;
//			} else {
//				table.close();
//				return records;
//			}
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		return null;
//
//	}
//	
//	public static Map<Integer, String> getColumnNames(String tableName) {
//		Map<Integer, String> columns = new LinkedHashMap<Integer, String>();
//		try {
//			RandomAccessFile table = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
//			int noOfPages = (int) (table.length() / PAGESIZE);
//
//			for (int i = 0; i < noOfPages; i++) {
//				table.seek(PAGESIZE * i);
//				byte pageType = table.readByte();
//				if (pageType == 0x0D) {
//
//					int noOfBlds = table.readByte();
//					short[] BldLocn = new short[noOfBlds];
//					table.seek((PAGESIZE * i) + 8);
//					for (int location = 0; location < noOfBlds; location++) {
//						BldLocn[location] = table.readShort();
//					}
//					Map<Integer, TablePage> recordBuilders = new LinkedHashMap<Integer, TablePage>();
//					recordBuilders = retrievePayload(table, BldLocn, i);
//
//					for (Map.Entry<Integer, TablePage> entry : recordBuilders.entrySet()) {
//
//						TablePage PageNav = entry.getValue();
//
//						TablePage payload = getPayload();
//						String[] data = payload.data;
//						if (data[0].equalsIgnoreCase(tableName)) {
//							columns.put(Integer.parseInt(data[3]), data[1]);
//						}
//
//					}
//
//				}
//
//			}
//			table.close();
//		} catch (Exception e) {
//
//			e.printStackTrace();
//		}
//
//		return columns;
//
//	}
	

}
