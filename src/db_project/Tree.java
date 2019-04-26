package db_project;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.Optional;

public class Tree{
	public static final int PAGESIZE=512;

	public static void splitLeafPage(RandomAccessFile table,int currentPage) {
		int newPage=createNewPage(table);
		int midKey=divideData(table,currentPage);
		moveRecords(table,currentPage,newPage,midKey);	
	}
	
	private static void moveRecords(RandomAccessFile table,int currentPage,int newPage,int midKey) {
		try{

			table.seek((currentPage)*PAGESIZE);
			byte pageType = table.readByte();
			int noOfCells = table.readByte();
			
			int mid = (int) Math.ceil(noOfCells/2);
			
			int lower = mid-1;
			int upper = noOfCells - lower;
			int content = 512;

			for(int i = mid; i <= noOfCells; i++){
				
				table.seek(currentPage*PAGESIZE+8+(2*i)-2);
				short offset=table.readShort();
				table.seek(offset);
				
				int cellSize = table.readShort()+6;
				content = content - cellSize;
				
				table.seek(offset);
				byte[] cell = new byte[cellSize];
				table.read(cell);
			
				table.seek((newPage-1)*PAGESIZE+content);
				table.write(cell);
				
				table.seek((newPage-1)*PAGESIZE+8+(i-mid)*2);
				table.writeShort((newPage-1)*PAGESIZE+content);
				
			}

			// cell start area
			table.seek((newPage-1)*PAGESIZE+2);
			table.writeShort((newPage-1)*PAGESIZE+content);

			
			//current page cell content area update
			table.seek((currentPage)*PAGESIZE+8+(lower*2));
			short offset=table.readShort();
			table.seek((currentPage)*PAGESIZE+2);
			table.writeShort(offset);

			
			
			//copy right pointer of current page to new page
			table.seek((currentPage)*PAGESIZE+4);
			int rightpointer = table.readInt();
			table.seek((newPage-1)*PAGESIZE+4);
			table.writeInt(rightpointer);
			//update current page
			table.seek((currentPage)*PAGESIZE+4);
			table.writeInt(newPage); //CHECK HERE NP
			
			byte cells = (byte) lower;
			table.seek((currentPage)*PAGESIZE+1);
			table.writeByte(cells);
			cells = (byte) upper;
			table.seek((newPage-1)*PAGESIZE+1);
			table.writeByte(cells);
			
			//parent updation
			int parent =getParent(table,currentPage+1);
			if(parent==0){
				int parentpage = createInteriorPage(table);
				setParent(table,parentpage,currentPage,midKey);
				table.seek((parentpage-1)*PAGESIZE+4);
				table.writeInt(newPage); // right child
			}
			else
			{
				if(checkforRightPointer(table,parent,currentPage+1))
				{
					setParent(table,parent,currentPage,midKey);
					table.seek((parent-1)*PAGESIZE+4);
					table.writeInt(newPage); // right child
				}
				else{
					setParent(table,parent,newPage,midKey);
				}
			}
		}catch(Exception e){
			System.out.println("Error at splitLeafPage");
			e.printStackTrace();
		}
	}
	
	
	private static void setParent(RandomAccessFile table, int parent,  int childPage, int midkey) {
		// TODO Auto-generated method stub
		try {
			table.seek((parent-1)*PAGESIZE+1);
			int numrecords = table.read();
			if(checkInteriorRecordFit(table,parent))
			{
				
				int content=(parent)*PAGESIZE;
				TreeMap<Integer,Short> offsets = new TreeMap<Integer,Short>();
				if(numrecords==0){
					table.seek((parent-1)*PAGESIZE+1);
					table.write(1);
					content = content-8;
					table.writeShort(content);  //cell content star
					table.writeInt(-1);		//right page pointer
					table.writeShort(content);	//offset arrays
					table.seek(content);
					table.writeInt(childPage+1);
					table.writeInt(midkey);

				}
				else{
					table.seek((parent-1)*PAGESIZE+2);
					short cellContentArea = table.readShort();
					cellContentArea = (short) (cellContentArea-8);
					table.seek(cellContentArea);
					table.writeInt(childPage+1);
					table.writeInt(midkey);
					table.seek((parent-1)*PAGESIZE+2);
					table.writeShort(cellContentArea);
					for(int i=0;i<numrecords;i++){
						table.seek((parent-1)*PAGESIZE+8+2*i);
						short off = table.readShort();
						table.seek(off+4);
						int key = table.readInt();
						offsets.put(key, off);
					}
					offsets.put(midkey,cellContentArea);
					table.seek((parent-1)*PAGESIZE+1);
					table.write(numrecords++);
					table.seek((parent-1)*PAGESIZE+8);
					for(Entry<Integer, Short> entry : offsets.entrySet()) {
						table.writeShort(entry.getValue());
					}
				}
			}
			else{
				splitInteriorPage(table,parent);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	private static void splitInteriorPage(RandomAccessFile table, int parent) {
		
		
		int newPage = createInteriorPage(table);
		int midKey = divideData(table, parent-1);
		writeContentInteriorPage(table,parent,newPage,midKey);
		
		
		try {
			table.seek((parent-1)*PAGESIZE+4);
			int rightpage = table.readInt();
			table.seek((newPage-1)*PAGESIZE+4);
			table.writeInt(rightpage);
			table.seek((parent-1)*PAGESIZE+4);
			table.writeInt(newPage);
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}
	
	private static void writeContentInteriorPage(RandomAccessFile table, int parent, int newPage, int midKey) {
		// TODO Auto-generated method stub
		try {
			table.seek((parent-1)*PAGESIZE+1);
			int numrecords = table.read();
			int mid = (int) Math.ceil((double)numrecords/2);
			int numrecords1 = mid-1;
			int numrecords2 = numrecords-numrecords1;
			int size = PAGESIZE;
			for(int i=numrecords1;i<numrecords;i++)
			{
				table.seek((parent-1)*PAGESIZE+8+2*i);
				short offset = table.readShort();
				table.seek(offset);
				byte[] data = new byte[8];
				table.read(data);
				size = size-8;
				table.seek((newPage-1)*PAGESIZE+size);
				table.write(data);
				
				//setting offset
				table.seek((newPage-1)*PAGESIZE+8+(i-numrecords1)*2);
				table.writeShort(size);
				
			}
			
			//setting number of records
			table.seek((parent-1)*PAGESIZE+1);
			table.write(numrecords1);
			
			table.seek((newPage-1)*PAGESIZE+1);
			table.write(numrecords2);
			
			int int_parent = getParent(table, parent);
			if(int_parent==0){
				int newParent = createInteriorPage(table);
				setParent(table, newParent, parent, midKey);
				table.seek((newParent-1)*PAGESIZE+4);
				table.writeInt(newPage);
			}
			else{
				if(checkforRightPointer(table,int_parent,parent)){
					setParent(table, int_parent, parent, midKey);
					table.seek((int_parent-1)*PAGESIZE+4);
					table.writeInt(newPage);
				}
				else
				setParent(table, int_parent, newPage, midKey);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static boolean checkforRightPointer(RandomAccessFile table, int parent, int rightPointer) {
		
		try {
			table.seek((parent-1)*PAGESIZE+4);
			if(table.readInt()==rightPointer)
				return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	private static boolean checkInteriorRecordFit(RandomAccessFile table, int parent) {
		
		try {
			table.seek((parent - 1) * PAGESIZE + 1);
			int numrecords = table.read();
			short cellcontent = table.readShort();
			int size = 8 + numrecords * 2 + cellcontent;
			size = PAGESIZE - size;
			if (size >= 8)
				return true;
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		return false;
	}
	
	
	private static int createInteriorPage(RandomAccessFile table) {
		
		int numpages =0;
		try {
			numpages= (int) (table.length()/PAGESIZE);
			numpages++;
			table.setLength(table.length()+PAGESIZE);
			table.seek((numpages-1)*PAGESIZE);
			table.write(0x05);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return numpages;
	}
	
	private static int divideData(RandomAccessFile table,int pageNo)
	{
		int midKey=0;
		try{
			table.seek((pageNo)*PAGESIZE);
			byte pageType = table.readByte();
			short numCells = table.readByte();
			// id of mid cell
			short mid = (short) Math.ceil(numCells/2);
			
			table.seek(pageNo*PAGESIZE+8+(2*(mid-1)));
			short addr=table.readShort();
			table.seek(addr);
			
			if(pageType==0x0D)
				table.seek(addr+2);
			else 
				table.seek(addr+4);
		
			midKey=table.readInt();
			
			
		}catch(Exception e){
			e.printStackTrace();
		}

		return midKey;

	}

	private static int createNewPage(RandomAccessFile table) {
		
		try{
			int noOfPages = (int)table.length()/PAGESIZE;
			noOfPages = noOfPages + 1;
			table.setLength(noOfPages*PAGESIZE);
			table.seek((noOfPages-1)*PAGESIZE);
			table.writeByte(0x0D);
			return noOfPages;
		}catch(Exception e){
			e.printStackTrace();
		}

		return -1;
	}


	public static void writePayload(RandomAccessFile file, Record cell,int cellLocation)
	{
		
		try{
			
		file.seek(cellLocation);
		file.writeShort(cell.getPayLoadSize());
		file.writeInt(cell.getRowId());	
		file.writeByte(cell.columnCount);
		
		byte[] dataTypes=cell.getColDataTypes();
		file.write(dataTypes);
		
		String data[]=cell.getData();
		
		for(int i = 0; i < dataTypes.length; i++){
			switch(dataTypes[i]){
				case 0x00:
					file.writeByte(0);
					break;
				case 0x01:
					file.writeShort(0);
					break;
				case 0x02:
					file.writeInt(0);
					break;
				case 0x03:
					file.writeLong(0);
					break;
				case 0x04:
					file.writeByte(new Byte(data[i+1]));
					break;
				case 0x05:
					file.writeShort(new Short(data[i+1]));
					break;
				case 0x06:
					file.writeInt(new Integer(data[i+1]));
					break;
				case 0x07:
					file.writeLong(new Long(data[i+1]));
					break;
				case 0x08:
					file.writeFloat(new Float(data[i+1]));
					break;
				case 0x09:
					file.writeDouble(new Double(data[i + 1]));
					break;
				case 0x0A:
					long datetime = file.readLong();
					ZoneId zoneId = ZoneId.of("America/Chicago");
					Instant x = Instant.ofEpochSecond(datetime);
					ZonedDateTime zdt2 = ZonedDateTime.ofInstant(x, zoneId);
					zdt2.toLocalTime();
					// file.writeBytes(zdt2.toLocalDateTime().toString());
					break;
				case 0x0B:
					long date = file.readLong();
					ZoneId zoneId1 = ZoneId.of("America/Chicago");
					Instant x1 = Instant.ofEpochSecond(date);
					ZonedDateTime zdt3 = ZonedDateTime.ofInstant(x1, zoneId1);
					// file.writeBytes(zdt3.toLocalDate().toString());
					break;
				default:
					file.writeBytes(data[i + 1]);
					break;
			}
		}
		
		//update no of cells
		file.seek((PAGESIZE*cell.getPageNo())+1);
		int noOfCells = file.readByte();
		
		file.seek((PAGESIZE*cell.getPageNo())+1);
		file.writeByte((byte)(noOfCells+1));
		
		
		//update cell start offset
		
		//update cell arrays
		//getPositionMethod
		Map<Integer,Short> updateMap=new TreeMap<Integer,Short>();
		short[] cellLocations = new short[noOfCells];
		int[] keys=new int[noOfCells];
		
		for (int location = 0; location < noOfCells; location++) {
			
			file.seek((PAGESIZE * cell.getPageNo())+8+(location*2));
			cellLocations[location] = file.readShort();
			file.seek(cellLocations[location]+2);
			keys[location]=file.readInt();
			updateMap.put(keys[location], cellLocations[location]);
		}
		updateMap.put(cell.getRowId(), (short)cellLocation);
		
		//update Cell Array in ascending order
		file.seek((PAGESIZE * cell.getPageNo()) + 8);
		for (Map.Entry<Integer, Short> entry : updateMap.entrySet()) {
			short offset=entry.getValue();
			file.writeShort(offset);
		}
		
		
		//update cell start area
		file.seek((PAGESIZE * cell.getPageNo())+2);
		file.writeShort(cellLocation);
		file.close();
		
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}	
	}
	
	public static Record createCell(int pageNo,int primaryKey,short payLoadSize,byte[] dataType,String[] values)
	{
		Record cell=new Record();
		cell.setPageNo(pageNo);
		cell.setPageNo(primaryKey);
		cell.setPayLoadSize(payLoadSize);		
		cell.setColumnCount(Byte.parseByte(values.length-1+""));
		cell.setColDataTypes(dataType);
		cell.setData(values);	
		return cell;
	}
	

	public static int checkPageOverFlow(RandomAccessFile file, int page, int payLoadsize){
		int val = -1;

		try{
			file.seek((page)*PAGESIZE+2);
			int content = file.readShort();
			if(content == 0)
				return PAGESIZE - payLoadsize;
			
			file.seek((page)*PAGESIZE+1);
			int noOfCells=file.read();
			int pageHeaderSize=8+2*noOfCells+2;
			
			file.seek((page)*PAGESIZE+2);
			short startArea =(short)((page+1)*PAGESIZE- file.readShort());
			
			int space=startArea+pageHeaderSize;
			int spaceAvail = PAGESIZE-space;
			
			if(spaceAvail>=payLoadsize){
				file.seek((page)*PAGESIZE+2);
				short offset=file.readShort();
				return offset-payLoadsize;
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}

		return val;
	}
	
	public static int getParent(RandomAccessFile table, int page) {

		try {
			int numpages = (int) (table.length() / PAGESIZE);
			for (int i = 0; i < numpages; i++) {

				table.seek(i * PAGESIZE);
				byte pageType = table.readByte();

				if (pageType == 0x05) {
					table.seek(i * PAGESIZE + 4);
					int p = table.readInt();
					if (page == p)
						return i + 1;

					table.seek(i * PAGESIZE + 1);
					int numrecords = table.read();
					short[] offsets = new short[numrecords];

					for (int j = 0; j < numrecords; j++) {
						table.seek(i * PAGESIZE + 8 + 2 * j);
						offsets[i] = table.readShort();
						table.seek(offsets[i]);
						if (page == table.readInt())
							return j + 1;

					}

				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}
}
