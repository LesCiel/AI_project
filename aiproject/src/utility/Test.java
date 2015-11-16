package utility;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;

public class Test {
	
	public static void main(String args[]) throws IOException{
		
		BufferedReader reader = new BufferedReader(new FileReader("/home/teng/aiproject/ohio_tweets.csv"));
		String line = null;
		PrintStream ps = new PrintStream(new File("/home/teng/aiproject/tweets.csv"));
		String tokens[];
		while((line=reader.readLine())!=null){
			tokens = line.substring(6, line.length()-1).split(" ");
			ps.println(tokens[0]+"\t"+tokens[1]);
		}
		ps.close();
		reader.close();
		
	}

}
