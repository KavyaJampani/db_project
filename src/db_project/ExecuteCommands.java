package db_project;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Map;


public class ExecuteCommands {

    static short pageSize = 512;
    static DavisBaseHelper dbHelper;


    public static void displayQuery(String tableName) {
        try{
            String fileName = "data";
            if (tableName.equals("davisbase_tables") || tableName.equals("davisbase_columns"))
                fileName += ("/catalog/" + tableName + ".tbl");
            else
                fileName += ("/userdata/" + tableName + ".tbl");

            RandomAccessFile table = new RandomAccessFile(fileName, "rw");

            int pageCount = (int) (table.length() / pageSize);
            byte pageStart = 0;

            Map<String,String> columnPairs = dbHelper.getColumnNames(tableName);
            dbHelper.displayColumns(columnPairs);

            Page page = new Page();

            for (int x = 0; x < pageCount; x++) {
                table.seek(pageSize * x);
                byte pageType = table.readByte();
                if (pageType == 0x0D) {
                    pageStart = (byte)(pageSize * page.pageNo);
                    page = dbHelper.retrievePageDetails(table, pageStart);

                    for (Record record : page.records){
                        System.out.println(record.displayRow());
                    }
                }
            }
            table.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
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
                String[] insertValues = new String[]{tableName, columnInfo[0], columnInfo[1].toUpperCase(), "YES", Integer.toString(i)};
                if (columnInfo.length > 2)
                    if (columnInfo[2].equals("NOT"))
                        insertValues[3] = "NO";

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


    public static void insertRecord(RandomAccessFile table, String[] insertValues) {

        try{

            short payloadLength = 0;

            for (int i = 0; i < insertValues.length; i++)
                payloadLength += insertValues[i].length() ;
            short recordHeaderLength = (short) (1 + insertValues.length);
            short totalRecordLength = (short) (recordHeaderLength + payloadLength);
            short recordSpace = (short) (2 + 4 + totalRecordLength);

            int pageCount = (int) (table.length() / pageSize);

            Page page;

            for (int x = 0; x < pageCount; x++) {
                table.seek(pageSize * x);

                byte pageType = table.readByte();
                if (pageType == 0x0D) {

                    page = dbHelper.retrievePageDetails(table, x);

                    short oldStartLocation= page.startLocation;

                    int prevRowid;
                    if(page.recordCount == 0){
                        prevRowid = 0;

                    }
                    else{
                        //TODO: access it through record class
                        table.seek(oldStartLocation+2);
                        prevRowid = table.readInt();
                    }

                    //update record locations and start location
                    page.addRecordLocation(table, recordSpace);

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

                    page.incrementRecordCount(table);

                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

}

