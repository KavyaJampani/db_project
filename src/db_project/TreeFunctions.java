package db_project;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map.Entry;
import java.util.TreeMap;

public class TreeFunctions {
	public static int PAGESIZE=512;
	
	public static void writeToPage(RandomAccessFile table, int parent, int newPage, int midKey) {
		try {
			table.seek((parent - 1) * PAGESIZE + 1);
			int noRecs = table.read();
			int mid = (int) Math.ceil((double) noRecs / 2);
			int noRecs1 = mid - 1;
			int noRecs2 = noRecs - noRecs1;
			int size = PAGESIZE;
			for (int i = noRecs1; i < noRecs; i++) {
				table.seek((parent - 1) * PAGESIZE + 8 + 2 * i);
				short offset = table.readShort();
				table.seek(offset);
				byte[] data = new byte[8];
				table.read(data);
				size = size - 8;
				table.seek((newPage - 1) * PAGESIZE + size);
				table.write(data);

				table.seek((newPage - 1) * PAGESIZE + 8 + (i - noRecs1) * 2);
				table.writeShort(size);

			}

			table.seek((parent - 1) * PAGESIZE + 1);
			table.write(noRecs1);

			table.seek((newPage - 1) * PAGESIZE + 1);
			table.write(noRecs2);

			int tree_parent = TreeFunctions.getParent(table, parent);
			if (tree_parent == 0) {
				int new_tree_parent = createInteriorPage(table);
				TreeFunctions.setParent(table, new_tree_parent, parent, midKey);
				table.seek((new_tree_parent - 1) * PAGESIZE + 4);
				table.writeInt(newPage);
			} else {
				if (rightPointer(table, tree_parent, parent)) {
					TreeFunctions.setParent(table, tree_parent, parent, midKey);
					table.seek((tree_parent - 1) * PAGESIZE + 4);
					table.writeInt(newPage);
				} else
					TreeFunctions.setParent(table, tree_parent, newPage, midKey);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static boolean rightPointer(RandomAccessFile file, int parent, int rightpointer)
	{
		try {
			file.seek((parent-1)*PAGESIZE +4);
			if(file.readInt()== rightpointer)
				return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	public static boolean checkCapacity(RandomAccessFile file, int parent){
		try{
			file.seek((parent-1)*PAGESIZE +1);
			int norecs = file.read();
			short Buildercontent = file.readShort();
			int size= 8 + norecs*2 +Buildercontent;
			size = PAGESIZE - size;
			if (size>7)
				return true;
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return false;
	}

	
	
	public static int createInteriorPage(RandomAccessFile file)
	{
		int noOfPages=0;
		try {
			noOfPages=(int) ( file.length()/ PAGESIZE);
			noOfPages++;
			file.setLength(file.length()+PAGESIZE);
			file.seek((noOfPages-1) * PAGESIZE);
			file.write(0x05);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return noOfPages;
		
	}
	public static int createLeafPage(RandomAccessFile file)
	{
		int noOfPages=0;
		try {
			noOfPages = (int) (file.length()/PAGESIZE);
			noOfPages++;
			file.setLength(file.length()+PAGESIZE);
			file.seek((noOfPages-1) * PAGESIZE);
			file.write(0x0D);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return noOfPages;
	}
	
	
	public static void setParent(RandomAccessFile table, int parent, int childPage, int midkey) {
		try {
			table.seek((parent - 1) * PAGESIZE + 1);
			int noOfRecs = table.read();
			if (checkCapacity(table, parent)) {

				int content = (parent) * PAGESIZE;
				TreeMap<Integer, Short> offsets = new TreeMap<Integer, Short>();
				if (noOfRecs == 0) {
					table.seek((parent - 1) * PAGESIZE + 1);
					table.write(1);
					content = content - 8;
					table.writeShort(content);
					table.writeInt(-1);
					table.writeShort(content);
					table.seek(content);
					table.writeInt(childPage + 1);
					table.writeInt(midkey);

				} else {
					table.seek((parent - 1) * PAGESIZE + 2);
					short BuilderContentArea = table.readShort();
					BuilderContentArea = (short) (BuilderContentArea - 8);
					table.seek(BuilderContentArea);
					table.writeInt(childPage + 1);
					table.writeInt(midkey);
					table.seek((parent - 1) * PAGESIZE + 2);
					table.writeShort(BuilderContentArea);
					for (int i = 0; i < noOfRecs; i++) {
						table.seek((parent - 1) * PAGESIZE + 8 + 2 * i);
						short off = table.readShort();
						table.seek(off + 4);
						int key = table.readInt();
						offsets.put(key, off);
					}
					offsets.put(midkey, BuilderContentArea);
					table.seek((parent - 1) * PAGESIZE + 1);
					table.write(noOfRecs++);
					table.seek((parent - 1) * PAGESIZE + 8);
					for (Entry<Integer, Short> entry : offsets.entrySet()) {
						table.writeShort(entry.getValue());
					}
				}
			} else {
				splitPage(table, parent);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}
		
	
	public static int getParent(RandomAccessFile table, int page) {

			try {
				int noOfPages = (int) (table.length() / PAGESIZE);
				for (int i = 0; i < noOfPages; i++) {

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
				e.printStackTrace();
			}
			return 0;
		}
		
		private static void changeValues(RandomAccessFile table, int currentPage, int newPage, int midKey) {
			try {

				table.seek((currentPage) * PAGESIZE);
				byte pageType = table.readByte();
				int noOfBuilders = table.readByte();

				int mid = (int) Math.ceil(noOfBuilders / 2);

				int lower = mid - 1;
				int upper = noOfBuilders - lower;
				int content = 512;

				for (int i = mid; i <= noOfBuilders; i++) {

					table.seek(currentPage * PAGESIZE + 8 + (2 * i) - 2);
					short offset = table.readShort();
					table.seek(offset);

					int BuilderSize = table.readShort() + 6;
					content = content - BuilderSize;

					table.seek(offset);
					byte[] Builder = new byte[BuilderSize];
					table.read(Builder);

					table.seek((newPage - 1) * PAGESIZE + content);
					table.write(Builder);

					table.seek((newPage - 1) * PAGESIZE + 8 + (i - mid) * 2);
					table.writeShort((newPage - 1) * PAGESIZE + content);

				}

				table.seek((newPage - 1) * PAGESIZE + 2);
				table.writeShort((newPage - 1) * PAGESIZE + content);

				table.seek((currentPage) * PAGESIZE + 8 + (lower * 2));
				short offset = table.readShort();
				table.seek((currentPage) * PAGESIZE + 2);
				table.writeShort(offset);

				table.seek((currentPage) * PAGESIZE + 4);
				int rightpointer = table.readInt();
				table.seek((newPage - 1) * PAGESIZE + 4);
				table.writeInt(rightpointer);
				table.seek((currentPage) * PAGESIZE + 4);
				table.writeInt(newPage);

				byte Builders = (byte) lower;
				table.seek((currentPage) * PAGESIZE + 1);
				table.writeByte(Builders);
				Builders = (byte) upper;
				table.seek((newPage - 1) * PAGESIZE + 1);
				table.writeByte(Builders);

				int parent = TreeFunctions.getParent(table, currentPage + 1);
				if (parent == 0) {
					int parentpage = createInteriorPage(table);
					TreeFunctions.setParent(table, parentpage, currentPage, midKey);
					table.seek((parentpage - 1) * PAGESIZE + 4);
					table.writeInt(newPage);
				} else {
					if (rightPointer(table, parent, currentPage + 1)) {
						TreeFunctions.setParent(table, parent, currentPage, midKey);
						table.seek((parent - 1) * PAGESIZE + 4);
						table.writeInt(newPage);
					} else {
						TreeFunctions.setParent(table, parent, newPage, midKey);
					}
				}
			} catch (Exception e) {
				System.out.println("Error at splitLeafPage");
				e.printStackTrace();
			}
		}
		
		public static void splitLeaf(RandomAccessFile table, int currentPage) {
			int newPage = createLeafPage(table);
			int midKey = splitData(table, currentPage);
			changeValues(table, currentPage, newPage, midKey);
		}
		
		
		private static int splitData(RandomAccessFile table, int pageNo) {
			int midKey = 0;
			try {
				table.seek((pageNo) * PAGESIZE);
				byte pageType = table.readByte();
				short numBuilders = table.readByte();
				short mid = (short) Math.ceil(numBuilders / 2);

				table.seek(pageNo * PAGESIZE + 8 + (2 * (mid - 1)));
				short addr = table.readShort();
				table.seek(addr);

				if (pageType == 0x0D)
					table.seek(addr + 2);
				else
					table.seek(addr + 4);
				midKey = table.readInt();
			} catch (Exception e) {
				e.printStackTrace();
			}

			return midKey;

		}
		
		private static void splitPage(RandomAccessFile table, int parent) {

			int newPage = createInteriorPage(table);
			int midKey = splitData(table, parent - 1);
			writeToPage(table, parent, newPage, midKey);

			try {
				table.seek((parent - 1) * PAGESIZE + 4);
				int rightpage = table.readInt();
				table.seek((newPage - 1) * PAGESIZE + 4);
				table.writeInt(rightpage);
				table.seek((parent - 1) * PAGESIZE + 4);
				table.writeInt(newPage);
			} catch (IOException e) {

				e.printStackTrace();
			}
		}
	
	public static int checkOverFlow(RandomAccessFile file, int pageNo, int payLoadSize)
	{
		int overFlow=-1;
		try
		{
			file.seek((pageNo*PAGESIZE) + 1);
			int temp=file.read();
			int pageHeader= 8+(temp*2)+2;
			
			file.seek((pageNo*PAGESIZE)+2);
			short contentArea= (short) (((pageNo+1)*PAGESIZE)-file.readShort());
			
			int  spaceAvailable= PAGESIZE-(pageHeader+contentArea);
			if(spaceAvailable>= payLoadSize)
			{
				file.seek(pageNo);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return overFlow;
	}

}
