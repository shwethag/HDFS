package mapreduce.mapper;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class Mapper implements IMapper {
	private static final String SEARCH_FILE = "./config/searchword.ini";
	private static String searchterm = null; 
	static {
		Scanner sc = null;
		try{
			sc = new Scanner(new File(SEARCH_FILE));
			if(sc.hasNext())
				searchterm = sc.nextLine();
			
		}catch(IOException e){
			System.out.println("Exception in reading searchword file");
		}
	}
	public Mapper() {
		
	}

	@Override
	public String map(String line) {
		if(searchterm == null)
			return line;
		if(line.contains(searchterm))
			return line;
		return null;
	}

}
