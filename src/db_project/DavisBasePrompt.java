package db_project;
import java.io.RandomAccessFile;
import java.nio.file.attribute.AclEntry.Builder;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.lang.System.out;

/**
 *  @author Chris Irwin Davis
 *  @version 1.0
 *  <b>
 *  <p>This is an example of how to create an interactive prompt</p>
 *  <p>There is also some guidance to get started wiht read/write of
 *     binary data files using RandomAccessFile class</p>
 *  </b>
 *
 */
public class DavisBasePrompt {

	static DavisBaseHelper db_helper;

	/* This can be changed to whatever you like */
	static String prompt = "davisql> ";
	static String version = "v1.0b(example)";
	static String copyright = "Â©2016 Chris Irwin Davis";
	static boolean isExit = false;
	/*
	 * Page size for alll files is 512 bytes by default.
	 * You may choose to make it user modifiable
	 */
	static int pageSize = 512;

	/*
	 *  The Scanner class is used to collect user commands from the prompt
	 *  There are many ways to do this. This is just one.
	 *
	 *  Each time the semicolon (;) delimiter is entered, the userCommand
	 *  String is re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	/** ***********************************************************************
	 *  Main method
	 */
	public static void main(String[] args) {

		/* Display the welcome screen */
		splashScreen();

		/* Variable to collect user input from the prompt */
		String userCommand = "";

		initializeDataStore();

		while(!isExit) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");


	}

	/** ***********************************************************************
	 *  Static method definitions
	 */

	/**
	 *  Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-",80));
		System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}

	public static void initializeMetaTable() {
		try {
			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");

			int payloadLength = "davisbase_tables".length();
			int recordHeaderLength = 1 + 1;
			int totalRecordLength = recordHeaderLength+ payloadLength;
			int recordSpace = 2 + 4 + totalRecordLength;

			int payloadLength2 = "davisbase_columns".length();
			int recordHeaderLength2 = 1 + 1;
			int totalRecordLength2 = recordHeaderLength2+ payloadLength2;
			int recordSpace2 = 2 + 4 + totalRecordLength2;


			davisbaseTablesCatalog.setLength(pageSize);

			// Header
			davisbaseTablesCatalog.seek(0);
			// Set Page Type
			davisbaseTablesCatalog.write(0x0D);
			// Set Number of Records
			davisbaseTablesCatalog.write(0x02);
			// Set Start of Content Location
			davisbaseTablesCatalog.writeShort(pageSize - recordSpace - recordSpace2 - 1);
			// Set Rightmost Leaf Page
			davisbaseTablesCatalog.writeInt(-1);
			// Store Array of Record Locations
			davisbaseTablesCatalog.writeShort(pageSize - recordSpace);
			davisbaseTablesCatalog.writeShort(pageSize - recordSpace  - recordSpace2 - 1);

			//Record 2
			davisbaseTablesCatalog.seek(pageSize - recordSpace - recordSpace2);
			// Set Length of Payload
			davisbaseTablesCatalog.writeShort(totalRecordLength2);
			// Set rowid
			davisbaseTablesCatalog.writeInt(2);
			// Set Number of Columns
			davisbaseTablesCatalog.write(0x01);
			// Store Array of Column Data Types
			davisbaseTablesCatalog.write(0x0C + "davisbase_columns".length());
			// Store List of Column Data Values
			davisbaseTablesCatalog.writeBytes("davisbase_columns");

			//Record 1
			//davisbaseTablesCatalog.seek(pageSize - recordSpace);

			// Set Length of Payload
			davisbaseTablesCatalog.writeShort(totalRecordLength);
			// Set rowid
			davisbaseTablesCatalog.writeInt(1);
			// Set Number of Columns
			davisbaseTablesCatalog.write(0x01);
			// Store Array of Column Data Types
			davisbaseTablesCatalog.write(0x0C + "davisbase_tables".length());
			// Store List of Column Data Values
			davisbaseTablesCatalog.writeBytes("davisbase_tables");

			davisbaseTablesCatalog.close();
		}
		catch (Exception e) {
			out.println("Unable to create the database_tables file");
			out.println(e);
		}

	}

	public static void insertRecord(){

	}

	public static void initializeMetaColumns() {
		try {
			RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");

			short[] offset = new short[7];
			short[] payloadLengths = {34,40,41,35,41,42,40};
			offset[0] = (short)(pageSize - payloadLengths[0]);
			for(int i = 1; i< payloadLengths.length; i++){
				offset[i] = (short)(offset[i-1] - payloadLengths[i]);
			}

			davisbaseColumnsCatalog.setLength(pageSize);

			// Header
			davisbaseColumnsCatalog.seek(0);
			// Set Page Type
			davisbaseColumnsCatalog.write(0x0D);
			// Set Number of Records
			davisbaseColumnsCatalog.write(7);
			// Set Start of Content Location
			davisbaseColumnsCatalog.writeShort(offset[offset.length-1]);
			// Set Rightmost Leaf Page
			davisbaseColumnsCatalog.writeInt(-1);
			// Store Array of Record Locations
			for (int i = 0; i< 7; i++){
				davisbaseColumnsCatalog.writeShort(offset[i]);
			}

			davisbaseColumnsCatalog.seek(offset[6]);
			//Record 7
			//davisbaseColumnsCatalog.seek(offset[0]);
			// Set Length of Payload
			davisbaseColumnsCatalog.writeShort(payloadLengths[2]);
			// Set rowid
			davisbaseColumnsCatalog.writeInt(7);
			// Set Number of Columns
			davisbaseColumnsCatalog.write(0x03);
			// Store Array of Column Data Types
			davisbaseColumnsCatalog.write(0x0C + "davisbase_columns".length());
			davisbaseColumnsCatalog.write(0x0C + "data_type".length());
			davisbaseColumnsCatalog.write(0x0C + "TEXT".length());
			// Store List of Column Data Values
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("data_type");
			davisbaseColumnsCatalog.writeBytes("TEXT");


			//Record 6
			//davisbaseColumnsCatalog.seek(offset[0]);
			// Set Length of Payload
			davisbaseColumnsCatalog.writeShort(payloadLengths[2]);
			// Set rowid
			davisbaseColumnsCatalog.writeInt(5);
			// Set Number of Columns
			davisbaseColumnsCatalog.write(0x03);
			// Store Array of Column Data Types
			davisbaseColumnsCatalog.write(0x0C + "davisbase_columns".length());
			davisbaseColumnsCatalog.write(0x0C + "column_name".length());
			davisbaseColumnsCatalog.write(0x0C + "TEXT".length());
			// Store List of Column Data Values
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("column_name");
			davisbaseColumnsCatalog.writeBytes("TEXT");

			//Record 5
			//davisbaseColumnsCatalog.seek(offset[0]);
			// Set Length of Payload
			davisbaseColumnsCatalog.writeShort(payloadLengths[2]);
			// Set rowid
			davisbaseColumnsCatalog.writeInt(5);
			// Set Number of Columns
			davisbaseColumnsCatalog.write(0x03);
			// Store Array of Column Data Types
			davisbaseColumnsCatalog.write(0x0C + "davisbase_columns".length());
			davisbaseColumnsCatalog.write(0x0C + "table_name".length());
			davisbaseColumnsCatalog.write(0x0C + "TEXT".length());
			// Store List of Column Data Values
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("table_name");
			davisbaseColumnsCatalog.writeBytes("TEXT");

			//Record 4
			//davisbaseColumnsCatalog.seek(offset[0]);
			// Set Length of Payload
			davisbaseColumnsCatalog.writeShort(payloadLengths[2]);
			// Set rowid
			davisbaseColumnsCatalog.writeInt(4);
			// Set Number of Columns
			davisbaseColumnsCatalog.write(0x03);
			// Store Array of Column Data Types
			davisbaseColumnsCatalog.write(0x0C + "davisbase_columns".length());
			davisbaseColumnsCatalog.write(0x0C + "rowid".length());
			davisbaseColumnsCatalog.write(0x0C + "INT".length());
			// Store List of Column Data Values
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("rowid");
			davisbaseColumnsCatalog.writeBytes("INT");

			//Record 3
			//davisbaseColumnsCatalog.seek(offset[0]);
			// Set Length of Payload
			davisbaseColumnsCatalog.writeShort(payloadLengths[2]);
			// Set rowid
			davisbaseColumnsCatalog.writeInt(3);
			// Set Number of Columns
			davisbaseColumnsCatalog.write(0x03);
			// Store Array of Column Data Types
			davisbaseColumnsCatalog.write(0x0C + "davisbase_tables".length());
			davisbaseColumnsCatalog.write(0x0C + "record_count".length());
			davisbaseColumnsCatalog.write(0x0C + "INT".length());
			// Store List of Column Data Values
			davisbaseColumnsCatalog.writeBytes("davisbase_tables");
			davisbaseColumnsCatalog.writeBytes("record_count");
			davisbaseColumnsCatalog.writeBytes("INT");


			//Record 2
			//davisbaseColumnsCatalog.seek(offset[0]);
			// Set Length of Payload
			davisbaseColumnsCatalog.writeShort(payloadLengths[1]);
			// Set rowid
			davisbaseColumnsCatalog.writeInt(2);
			// Set Number of Columns
			davisbaseColumnsCatalog.write(0x03);
			// Store Array of Column Data Types
			davisbaseColumnsCatalog.write(0x0C + "davisbase_tables".length());
			davisbaseColumnsCatalog.write(0x0C + "table_name".length());
			davisbaseColumnsCatalog.write(0x0C + "TEXT".length());
			// Store List of Column Data Values
			davisbaseColumnsCatalog.writeBytes("davisbase_tables");
			davisbaseColumnsCatalog.writeBytes("table_name");
			davisbaseColumnsCatalog.writeBytes("TEXT");


			//Record 1
			//davisbaseColumnsCatalog.seek(offset[0]);
			// Set Length of Payload
			davisbaseColumnsCatalog.writeShort(payloadLengths[0]);
			// Set rowid
			davisbaseColumnsCatalog.writeInt(1);
			// Set Number of Columns
			davisbaseColumnsCatalog.write(0x03);
			// Store Array of Column Data Types
			davisbaseColumnsCatalog.write(0x0C + "davisbase_tables".length());
			davisbaseColumnsCatalog.write(0x0C + "rowid".length());
			davisbaseColumnsCatalog.write(0x0C + "INT".length());
			// Store List of Column Data Values
			davisbaseColumnsCatalog.writeBytes("davisbase_tables");
			davisbaseColumnsCatalog.writeBytes("rowid");
			davisbaseColumnsCatalog.writeBytes("INT");

			davisbaseColumnsCatalog.close();
		}
		catch (Exception e) {
			out.println("Unable to create the database_tables file");
			out.println(e);
		}

	}


	public static void initializeDataStore() {
		try {
			File catalogDir = new File("data/catalog");
			File DataDir = new File("data/userdata");
			catalogDir.mkdirs();
			DataDir.mkdirs();
			if (catalogDir.isDirectory()) {
				File davisBaseTables = new File("data/catalog/davisbase_tables.tbl");
				File davisBaseColumns = new File("data/catalog/davisbase_columns.tbl");

			} else {
				out.println("Unable to create data container directory");
				
			}
		}
		catch (SecurityException se) {
			out.println("Unable to create data container directory");
			out.println(se);
		}

		initializeMetaTable();
		initializeMetaColumns();


	}
	


	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}

	public static void printCmd(String s) {
		System.out.println("\n\t" + s + "\n");
	}
	public static void printDef(String s) {
		System.out.println("\t\t" + s);
	}

	/**
	 *  Help: Display supported commands
	 */
	public static void help() {
		out.println(line("*",80));
		out.println("SUPPORTED COMMANDS\n");
		out.println("All commands below are case insensitive\n");
		out.println("SHOW TABLES;");
		out.println("\tDisplay the names of all tables.\n");
		//printCmd("SELECT * FROM <table_name>;");
		//printDef("Display all records in the table <table_name>.");
		out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
		out.println("\tDisplay table records whose optional <condition>");
		out.println("\tis <column_name> = <value>.\n");
		out.println("DROP TABLE <table_name>;");
		out.println("\tRemove table data (i.e. all records) and its schema.\n");
		out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		out.println("\tModify records data whose optional <condition> is\n");
		out.println("VERSION;");
		out.println("\tDisplay the program version.\n");
		out.println("HELP;");
		out.println("\tDisplay this help information.\n");
		out.println("EXIT;");
		out.println("\tExit the program.\n");
		out.println(line("*",80));
	}

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}

	public static String getCopyright() {
		return copyright;
	}

	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}

	public static void parseUserCommand (String userCommand) {

		/* commandTokens is an array of Strings that contains one token per array element
		 * The first token can be used to determine the type of command
		 * The other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement */
		// String[] commandTokens = userCommand.split(" ");
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));


		/*
		 *  This switch handles a very small list of hardcoded commands of known syntax.
		 *  You will want to rewrite this method to interpret more complex commands.
		 */
		switch (commandTokens.get(0)) {
			//DDL Commands
			case "show":
				System.out.println("CASE: SHOW");
				showTables();
				break;
			case "create":
				System.out.println("CASE: CREATE");
				parseCreateTable(userCommand);
				break;
			case "drop":
				System.out.println("CASE: DROP");
				parseDropTable(userCommand);
				break;
			//DML Commands
			case "insert":
				System.out.println("CASE: INSERT");
				parseInsert(userCommand);
				break;
			case "delete":
				System.out.println("CASE: DELETE");
				parseDelete(userCommand);
				break;
			case "update":
				System.out.println("CASE: UPDATE");
				parseUpdate(userCommand);
				break;
			//VDL Commands
			case "select":
				System.out.println("CASE: SELECT");
				DavisBaseHelper.parseQueryString(userCommand);
				break;
			case "exit":
				isExit = true;
				break;
			//Misc Commands
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "quit":
				isExit = true;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}


	private static void parseQuery(String userCommand) {
		// TODO Auto-generated method stub
		
	}

	public static void showTables() {

	}
	
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
				DavisBaseHelper.createTable(table, tableName, columnNames);
			} catch (FileNotFoundException e) {
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
		if (db_helper.findTable(tableName+".tbl"))
			db_helper.dropTable(tableName);
		else{
		    System.out.println("User Error: Table does not exist.");
        }
	}


	public static void parseInsert(String insertString) {
		System.out.println("\tParsing the string:\"" + insertString + "\"");
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(insertString.split(" ")));
		String columnList = createTableTokens.get(3).replaceAll("\\(", "").replaceAll("\\)", "");
		String tableFileName = createTableTokens.get(4) + ".tbl";

		ArrayList<String> values = new ArrayList<String>();
		for (int i = 6; i < createTableTokens.size(); i++)
			values.add(createTableTokens.get(i).replaceAll("\\(", "").replaceAll("\\)", "").replaceAll(",", ""));
		out.println(columnList);
		out.println(tableFileName);
		out.println(values);

	}
	public static void parseDelete(String deleteString) {
		System.out.println("\tParsing the string:\"" + deleteString + "\"");
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(deleteString.split(" ")));
		String tableFileName = createTableTokens.get(3) + ".tbl";
		String condition = createTableTokens.get(5);
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
	
	public static void Query(String tableName, String[] columnNames, String[] condition) {

		try {

			tableName = tableName.trim();
			String path = "data/userdata/" + tableName + ".tbl";
			if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
				path = "data/catalog/" + tableName + ".tbl";

			RandomAccessFile table = new RandomAccessFile(path, "rw");
			int noOfPages = (int) (table.length() / pageSize);

			Map<Integer, String> colNames = getColumnNames(tableName);
			Map<Integer, Builder> records = new LinkedHashMap<Integer, Builder>();
			for (int i = 0; i < noOfPages; i++) {
				table.seek(pageSize * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {

					int noOfBuilders = table.readByte();
					short[] BuilderLocations = new short[noOfBuilders];
					table.seek((pageSize * i) + 8);
					for (int location = 0; location < noOfBuilders; location++) {
						BuilderLocations[location] = table.readShort();
					}
					Map<Integer, Builder> recordBuilders = new LinkedHashMap<Integer, Builder>();
					recordBuilders = getRecords(table, BuilderLocations, i);
					records.putAll(recordBuilders);
				}
			}

			if (condition.length > 0) {
				Map<Integer, Builder> filteredRecords = filterRecords(colNames, records, columnNames, condition);
				printTable(colNames, filteredRecords);
			} else {
				if (records.isEmpty()) {
					System.out.println("Empty Set..");
				} else {
					printTable(colNames, records);
				}
			}
			table.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public static String[] check(String str) {

		String condition[] = new String[3];
		String values[] = new String[2];
		if (str.contains("=")) {
			values = str.split("=");
			condition[0] = values[0].trim();
			condition[1] = "=";
			condition[2] = values[1].trim();
		}

		if (str.contains(">")) {
			values = str.split(">");
			condition[0] = values[0].trim();
			condition[1] = ">";
			condition[2] = values[1].trim();
		}

		if (str.contains("<")) {
			values = str.split("<");
			condition[0] = values[0].trim();
			condition[1] = "<";
			condition[2] = values[1].trim();
		}

		if (str.contains(">=")) {
			values = str.split(">=");
			condition[0] = values[0].trim();
			condition[1] = ">=";
			condition[2] = values[1].trim();
		}

		if (str.contains("<=")) {
			values = str.split("<=");
			condition[0] = values[0].trim();
			condition[1] = "<=";
			condition[2] = values[1].trim();
		}

		if (str.contains("<>")) {
			values = str.split("<>");
			condition[0] = values[0].trim();
			condition[1] = "<>";
			condition[2] = values[1].trim();
		}

		return condition;
	}


	private static void printTable(Map<Integer, String> colNames, Map<Integer, Builder> filteredRecords) {
		// TODO Auto-generated method stub
		
	}

	private static Map<Integer, Builder> filterRecords(Map<Integer, String> colNames, Map<Integer, Builder> records,
			String[] columnNames, String[] condition) {
		// TODO Auto-generated method stub
		return null;
	}

	private static Map<Integer, Builder> getRecords(RandomAccessFile table, short[] builderLocations, int i) {
		// TODO Auto-generated method stub
		//Map<Integer, Builder> dummy = new HashMap();
		//return dummy;
		return null;
	}

	private static Map<Integer, String> getColumnNames(String tableName) {
		// TODO Auto-generated method stub
		return null;
	}



}