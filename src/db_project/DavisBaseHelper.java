package db_project;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

	public static Map<String,String>  getColumnNames(String tableName) {

        Map<String,String> columnPairs = new HashMap<String,String>();

        try{
			RandomAccessFile table = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");

            int pageCount = (int) (table.length() / pageSize);

            Page page = new Page();

            for (int x = 0; x < pageCount; x++) {
                table.seek(pageSize * x);
                byte pageType = table.readByte();
                if (pageType == 0x0D) {
                    page = retrievePage(table, x);

                    for (Record record : page.records){
                        if(record.data[0].equals(tableName)) {
                            columnPairs.put(record.data[1], record.data[2]);
                        }
                    }
                }
            }
            table.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}

		return columnPairs;

	}

	public static void displayColumns(Map<String,String> columnNames){

        String displayString = "\t Row ID ";
        for (String columnName : columnNames.keySet())
        {
            displayString += "\t" + columnName;
        }
        System.out.println(displayString);
	}

    public static Record retrieveRecords(RandomAccessFile table, short location){

        Record record = new Record();

        try{

            table.seek(location);
            record.location = location;
            record.payLoadSize = table.readShort();
            record.rowId = table.readInt();
            record.columnCount = table.readByte();
            record.colDataTypes = new byte[record.columnCount];
            for(int i = 0; i<record.columnCount; i++){
                record.colDataTypes[i] = table.readByte();
            }
            record.data = new String[record.columnCount];
            for(int i = 0; i<record.columnCount; i++){


                switch (record.colDataTypes[i]) {

                    //NULL
                    case 0x00:
                        record.data[i] = "null";
                        break;

                    //TINYINT
                    case 0x01:
                        record.data[i] = Integer.toString(table.readByte());
                        break;

                    //SMALLINT
                    case 0x02:
                        record.data[i] = Integer.toString(table.readShort());
                        break;

                    //INT
                    case 0x03:
                        record.data[i] = Integer.toString(table.readInt());
                        break;

                    //LONG
                    case 0x04:
                        record.data[i] = Long.toString(table.readLong());
                        break;

                    //FLOAT
                    case 0x05:
                        record.data[i] = Float.toString(table.readFloat());
                        break;

                    //YEAR
                    case 0x06:
                        record.data[i] = Integer.toString(table.readInt());
                        break;

                    //TIME
                    case 0x08:
                        long tmp = table.readLong();
                        Date date = new Date(tmp);
                        DateFormat dateFormat = new SimpleDateFormat("hh:mm:ss");
                        String strDate = dateFormat.format(date.getTime());
                        record.data[i] = strDate;
                        break;

                    //DATETIME
                    case 0x0A:
                        long tmp1 = table.readLong();
                        Date date1 = new Date(tmp1);
                        DateFormat dateFormat1 = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
                        String strDate1 = dateFormat1.format(date1.getTime());
                        record.data[i] = strDate1;
                        break;

                    //DATE
                    case 0x0B:
                        long tmp2 = table.readLong();
                        Date date2 = new Date(tmp2);
                        DateFormat dateFormat2 = new SimpleDateFormat("yyyy-mm-dd");
                        String strDate2 = dateFormat2.format(date2.getTime());
                        record.data[i] = strDate2;
                        break;

                    //TEXT
                    default:
                        int textLength = record.colDataTypes[i] - 0x0C;
                        byte[] letters = new byte[textLength];
                        for (int j = 0; j < textLength; j++)
                            letters[j] = table.readByte();
                        record.data[i] = new String(letters);
                        break;
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return record;
    }

	public static Page retrievePage(RandomAccessFile table, int pageNo ){

        Page page = new Page();
        byte pageStart = (byte)((pageSize * pageNo));

        try{
            page.pageNo = pageNo;
            table.seek(pageStart);
            page.pageType = table.readByte();
            page.recordCount = table.readByte();
            page.startLocation = table.readShort();
            page.rightSibling = table.readInt();
            page.recordLocations = new short[page.recordCount];
            for(int i = 0; i<page.recordCount; i++) {
                page.recordLocations[i] = table.readShort();
            }
            if(page.startLocation <= 0)
                page.startLocation = (short) (pageStart + pageSize);
            table.seek(page.startLocation);
            page.records = new Record[page.recordCount];
            for (int i = 0; i < page.recordCount; i++) {
                if (page.recordLocations[i] == -1) {
                    //i--;
                    continue;
                }
                page.records[i] = retrieveRecords(table, page.recordLocations[i]);
                page.records[i].pageNo = page.pageNo;
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return page;
    }

}

	