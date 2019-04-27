package db_project;

import java.io.File;
import java.io.RandomAccessFile;
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

            Map<String,String> columnPairs = dbHelper.getColumnNames(tableName);
            dbHelper.displayColumns(columnPairs);

            Page page = new Page();

            for (int x = 0; x < pageCount; x++) {
                table.seek(pageSize * x);
                byte pageType = table.readByte();
                if (pageType == 0x0D) {
                    page = dbHelper.retrievePage(table, x);

                    for (Record record : page.records){
                        if (record == null)
                            continue;
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


            Record record = searchforRecord("davisbase_tables", 0, tableName);


            Page page = dbHelper.retrievePage(davisbaseTablesCatalog, record.pageNo);
            System.out.println(record);
            System.out.println(page);

            for(int i = 0; i<page.recordCount; i++){
                if (page.records[i].location == record.location){
                    System.out.println("here");
                    System.out.println(record.location);
                    davisbaseTablesCatalog.seek((pageSize * page.pageNo) + 8 + (i*2));
                    davisbaseTablesCatalog.writeShort(-1);
                    page.decrementRecordCount(davisbaseTablesCatalog);

                    File file = new File("data/userdata/" + tableName +".tbl");
                    file.delete();

                }
            }

            davisbaseTablesCatalog.close();

        }
        catch(Exception e){
            e.printStackTrace();
        }

    }


    public static Record searchforRecord(String tableName, int columnIndex, String searchString){

        Record foundRecord = new Record((short)-1);
        try{
            String fileName = "data";
            if (tableName.equals("davisbase_tables") || tableName.equals("davisbase_columns"))
                fileName += ("/catalog/" + tableName + ".tbl");
            else
                fileName += ("/userdata/" + tableName + ".tbl");

            RandomAccessFile table = new RandomAccessFile(fileName, "rw");

            int pageCount = (int) (table.length() / pageSize);

//            int colIndex = 0;

//            Map<String,String> columnPairs = dbHelper.getColumnNames(tableName);
//            for (int i = 0; i < columnPairs.size(); i++) {
//                String col = columnPairs.get(i);
//                if (col.equals(columnName)){
//                    colIndex = i;
//                }
//            }

            Page page;

            for (int x = 0; x < pageCount; x++) {
                table.seek(pageSize * x);
                byte pageType = table.readByte();
                if (pageType == 0x0D) {
                    page = dbHelper.retrievePage(table, x);

                    for (Record record : page.records){

                        if (record.data[columnIndex].equals(searchString)) {
                            System.out.println("reached here");
                            System.out.println(record.data[columnIndex]);
                            foundRecord = record;
                            break;

                        }
                    }
                }
            }
            table.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }

        return foundRecord;

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

                    page = dbHelper.retrievePage(table, x);

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

