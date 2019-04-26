package db_project;

import java.io.IOException;
import java.io.RandomAccessFile;

public class TreeFunctions {
	public static int PAGESIZE=512;
	
	public static int checkOverflow(RandomAccessFile file, int recordSize)
	{
		int value=-1;
		try {
			file.seek(1);
			//reads number of cells
			int x=file.readShort();
			file.seek(3);
			//reads starting addr of cell content area
			int y=file.readShort();
			//2*x->offesets for each record, 11-> 9byte header+2 byte offset for the new record
			int spaceAvail=PAGESIZE-(2*x+11+y);
			if(spaceAvail>=recordSize)
			{
				return (y-recordSize); 
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return value;
	}
	
	public static int createNewPage(RandomAccessFile file)
	{
		try {
			int noOfPages=(int) (file.length()/PAGESIZE);
			noOfPages++;
			file.seek((noOfPages-1)*PAGESIZE);
			file.write(0x0D);
			return noOfPages;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}
	
	public static int createInteriorPage(RandomAccessFile file)
	{
		try {
			int noOfPages=(int) (file.length()/PAGESIZE);
			noOfPages++;
			file.seek((noOfPages-1)*PAGESIZE);
			file.write(0x05);
			return noOfPages;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;

	}

}
