package db_project;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;

public class ParseCommands {

    static ExecuteCommands executeCommand;
    static DavisBaseHelper dbHelper;

    /**
     *  Stub method for creating new tables
     *  @param createTableString is a String of the user input
     */
    public static void parseCreateTable(String createTableString) {
        System.out.println("Parsing the string:\"" + createTableString + "\"");
        ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));

        String tableName = createTableTokens.get(2);
        String[] temp = createTableString.replaceAll("\\(", "").replaceAll("\\)", "").split(tableName);
        String[] columnNames = temp[1].trim().split(",");

        for (int i = 0; i < columnNames.length; i++)
            columnNames[i] = columnNames[i].trim();

        if (DavisBaseHelper.findTable(tableName)) {
            System.out.println("Table " + tableName + " is already present.");
            System.out.println();
        } else {
            RandomAccessFile table;
            try {
                table = new RandomAccessFile("data/userdata/" + tableName + ".tbl", "rw");
                executeCommand.createTable(table, tableName, columnNames);
                table.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    /**
     *  Stub method for dropping tables
     *  @param dropTableString is a String of the user input
     */
    public static void parseDropTable(String dropTableString) {
        System.out.println("\tParsing the string:\"" + dropTableString + "\"");
        ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(dropTableString.split(" ")));
        String tableName = createTableTokens.get(2);
        if (tableName == "davisbase_tables" ||  tableName == "davisbase_columns") {
            System.out.println("User Error: Cannot drop the meta tables.");
            return;
        }
        if (executeCommand.searchforRecord("davisbase_tables", 0, tableName).location != -1)
            executeCommand.dropTable(tableName);
        else{
            System.out.println("User Error: Table does not exist.");
        }
    }


    public static void parseInsert(String insertString) {
        String[] insert = insertString.split(" ");
        String tableName = insert[2].trim();
        String values = insertString.split("values")[1].replaceAll("\\(", "").replaceAll("\\)", "").trim();

        String[] insertValues = values.split(",");
        for (int i = 0; i < insertValues.length; i++)
            insertValues[i] = insertValues[i].trim();

        if (!dbHelper.findTable(tableName+".tbl")) {
            System.out.println("Table " + tableName + " does not exist.");
            System.out.println();
            return;
        } else
            try {
                RandomAccessFile table = new RandomAccessFile("data/userdata/" + tableName + ".tbl", "rw");
                executeCommand.insertRecord(table, insertValues);
                table.close();
            }
            catch(Exception e){
                e.printStackTrace();
            }

    }
    public static void parseDelete(String deleteString) {
        System.out.println("\tParsing the string:\"" + deleteString + "\"");
        ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(deleteString.split(" ")));
        String tableFileName = createTableTokens.get(3) ;
        String columnName = createTableTokens.get(5);
        String recordToDelete = createTableTokens.get(7);
        executeCommand.deleteRecord(tableFileName, columnName, recordToDelete);
    }

    public static void parseUpdate(String updateString) {
        System.out.println("Parsing the string:\"" + updateString + "\"");
        ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(updateString.split(" ")));
        String tableFileName = createTableTokens.get(1) + ".tbl";
        String columnName = createTableTokens.get(3);
        String value = createTableTokens.get(5);
        String condition = createTableTokens.get(7);
    }

    /**
     *  Stub method for executing queries
     *  @param queryString is a String of the user input
     */
    public static void parseQuery(String queryString) {
        System.out.println("\tParsing the string:\"" + queryString + "\"");
        ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(queryString.split(" ")));
        String tableName = createTableTokens.get(3);
        executeCommand.displayQuery(tableName);
    }

}
