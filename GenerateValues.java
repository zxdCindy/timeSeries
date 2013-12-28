package question4;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generate time series data
 * @author cindyzhang
 *
 */
public class GenerateValues {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		ReadWriteFile myReadWrite = new ReadWriteFile();
		List<String> randomList = new ArrayList<String>();
		for(int i=0; i< 100000; i++){
			String str = "";
			str = randomTimeStamp().toString()+","+ randomInt().toString();
			randomList.add(str);
		}
		myReadWrite.writeFile("/Users/cindyzhang/Desktop/input.txt", randomList);
	}
	
	/**
	 * Random time stamp
	 * @return
	 */
	public static Timestamp randomTimeStamp(){
		long offset = Timestamp.valueOf("2000-01-01 00:00:00").getTime();
		long end = Timestamp.valueOf("2014-01-01 00:00:00").getTime();
		long diff = end - offset + 1;
		Timestamp rand = new Timestamp(offset + (long)(Math.random() * diff));
		return rand;
	}
	
	/**
	 * Random integer
	 * @return
	 */
	public static Integer randomInt(){
		Random randomGenerator = new Random();
		return randomGenerator.nextInt();
	}

}
