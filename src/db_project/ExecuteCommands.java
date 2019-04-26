package db_project;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Map;


public class ExecuteCommands {

    static int pageSize = 512;
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

