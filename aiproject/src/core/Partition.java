package core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.vividsolutions.jts.io.WKTReader;

import utility.MBR;
import utility.MyList;
import utility.Point;
import utility.Variable;

//single machine now.. to be created in mapreduce .. high priority. 

public class Partition extends FileParser{
	/**
	 * each thread read files at the same time, and sort their own records, and then merge them. the last one who merge the sort result 
	 * partition the sorted points into slices. and then each thread tile slices one by one. the last tiler output the result.
	 * to keep the completeness of the whole data set in case of sampling, no gap is allowed between tiles. 
	 * 
	 * first sort by x.. then y
	 * */
    public static double samplerate = 1;

	WKTReader wktreader = new WKTReader();
	public static boolean sorting = false;
	public static boolean complete = false;
	public static int geoindex = 0;
	public static Integer workers = 0;
	public static PrintStream out = System.out;
	public static PrintStream bufferout = System.out;
	public static PrintStream wktout = System.out;
	public static PrintStream wktbufferout = System.out;
	
	public static int xDim = 4;
	public static int yDim = 4; //balance the number.. too big.. then need more r-tree within each one
	public static MyList global = new MyList();
	public static int numPointsPerPartition;
	public static int numPointsPerSlice;
	MyList local = new MyList();
	public static LinkedList<ArrayList<Point>> waitinglist = new LinkedList<ArrayList<Point>>();
	public static ArrayList<ArrayList<MBR>> result = new ArrayList<ArrayList<MBR>>();
	public static int tcount = 0;
	/*this is the max boundary of the whole map, four numbers are xmin, xmax, ymin, ymax accordingly.
	 * these number can be modified manually. by fixing these number, we need not traverse the whole 
	 * dataset to find the minimum boundary rectangular.  
	 * */
	public static MBR globalmbr = new MBR(-84.820305,-80.518705,38.403423,42.327132);
	/*the source files are in wkt format are in X Y separated format, if is X Y coordinate format, 
	 * the indexes for x and y should be defined.
	 * */
	public static int xindex = 0;
	public static int yindex = 0;
	public Partition(int id) 
	{
		super(id);
		workers++;
	}
	/*currently we only take care of case of iphone and android*/
    boolean filter(String source)
    {
    	return source.equalsIgnoreCase("Twitter for iPhone")||source.equalsIgnoreCase("Twitter for Android");
    }
	@Override
	protected void parseline(String filename, String line) {
		if(count%4000000==0)/*just for convenience, nothing else*/
		{
			print("parsed "+count);
		}
		if(Math.random()>samplerate)return;
		String tokens[] = line.split("\t");
		if(tokens.length<geoindex)
			return;
		  com.vividsolutions.jts.geom.Point point = null;
		  try {
			point = wktreader.read(tokens[geoindex].replaceAll("\"", "")).getCentroid();
		  } catch (Exception e) {
			//e.printStackTrace();
		  }
		  if(point!=null&&point.isValid())
		  {
			local.add(new Point(point.getX(),point.getY()));
		  }
		
		
	}

	@Override
	protected void reduce() {
		
		Collections.sort(local, new Comparator<Point>(){
			@Override
			public int compare(Point p1, Point p2) {
			     if (p1.getX() > p2.getX()) return 1;
				 else if (p1.getX() < p2.getX()) return -1;
				 return 0;
            }	
		});
		synchronized(global)
		{
			global = global.mergeInOrder(local);
			System.out.println("Thread "+threadid+" finished sorting, added "+local.size()+", current sorted list size is "+global.size());
			local.clear();

		    if(--workers==0)
			{
				numPointsPerPartition = global.size() / (yDim * xDim);
			    numPointsPerSlice = numPointsPerPartition * yDim;
				for (int i = 0; i < xDim; i++) 
				{
			    	//each slice 
			    	int begin = i * numPointsPerSlice;
			    	int end = i == xDim - 1 ? global.size() : (i + 1) * numPointsPerSlice;
			    	ArrayList<Point> tmp = new ArrayList<Point>();
			    	for(int t=begin;t<end;t++)
			    	{
			    		tmp.add(global.get(t));
			    	}
			    	waitinglist.add(tmp);
					result.add(new ArrayList<MBR>());
			    }
				sorting = true;
			}
		}
	}
	@Override
	protected void afterreduce()
	{
		while(!sorting)
		{
			//System.out.println("Thread "+threadid+" sleep for 10 milisecond");
		    try {
               Thread.sleep(10);
            } catch (InterruptedException e) {}
		}
		 
		ArrayList<Point> slice = null;
		while(true)
		{
			 int index = 0;
			 synchronized(waitinglist)
			 {
				  index = xDim-waitinglist.size();
				  slice = waitinglist.poll();
			 }
			 if(slice==null)
			 {
				  return;
			 }
			 Collections.sort(slice, new Comparator<Point>(){
			   @Override
			   public int compare(Point p1, Point p2) {
		               if (p1.getY() > p2.getY()) return 1;
				       else if (p1.getY() < p2.getY()) return -1;
				    	return 0;

				}	
			  }); 
			 ArrayList<MBR> mbrs= new ArrayList<MBR>();
			  for (int j = 0; j < yDim; j++) {
			    	int begin = j * numPointsPerPartition;
			    	int end = j == yDim -1 ? slice.size() : (j + 1) * numPointsPerPartition;
			    	double xmin = Double.MAX_VALUE;
			    	double ymin = Double.MAX_VALUE;
			    	double xmax = -Double.MAX_VALUE;
			    	double ymax = -Double.MAX_VALUE;
			    	for (int k = begin; k < end; k++) {
			    		Point poi = slice.get(k);
			    		xmin = xmin > poi.getX() ? poi.getX() : xmin;
			    		ymin = ymin > poi.getY() ? poi.getY() : ymin;
			    		xmax = xmax < poi.getX() ? poi.getX() : xmax;
			    		ymax = ymax < poi.getY() ? poi.getY() : ymax;
			    	}
			    	mbrs.add(new MBR(xmin,xmax,ymin,ymax));
			  }
			  result.set(index,mbrs);
	   }
	}
	@Override
	protected void last() 
	{	
		if(complete)
		{
		  double curxmax = -Double.MAX_VALUE,curxmin = Double.MAX_VALUE,formerxmax = -Double.MAX_VALUE;

		  for(int x=0;x<xDim;x++)
		  {
			curxmax = -Double.MAX_VALUE;
			curxmin = Double.MAX_VALUE;
			  
			ArrayList<MBR> formerslice = null, curslice = null;;
			curslice = result.get(x);
			for(int y=0;y<yDim;y++)
			{
				MBR curmbr=null,formermbr = null;
				curmbr = curslice.get(y);
				curxmin = Math.min(curxmin, curmbr.xmin);
				curxmax = Math.max(curxmax, curmbr.xmax);
				if(y>0)
				{
					formermbr = curslice.get(y-1);
					formermbr.ymax = (formermbr.ymax+curmbr.ymin)/2.0;
					curmbr.ymin = formermbr.ymax;
					curslice.set(y-1, formermbr);
					if(y==yDim-1)
						curmbr.ymax = globalmbr.ymax;
				}
				else
				{
					curmbr.ymin = globalmbr.ymin;
				}
				curslice.set(y, curmbr);
			}
			
			if(x>0)
			{
			   double boundary = (curxmin+formerxmax)/2;
			   formerslice = result.get(x-1);
			   
			   for(int i=0;i<yDim;i++)
			   {
				   MBR tmpformer = formerslice.get(i);
				   tmpformer.xmax = boundary;
				   formerslice.set(i, tmpformer);
				   
				   MBR tmpcur = curslice.get(i);
				   tmpcur.xmin = boundary;
				   if(x==xDim-1)
					   tmpcur.xmax = globalmbr.xmax;
				   curslice.set(i, tmpcur);
			   }
			   result.set(x-1, formerslice);
			   
			}
			else
			{
				for(int i=0;i<yDim;i++)
				{
				    MBR tmpcur = curslice.get(i);
					tmpcur.xmin = globalmbr.xmin;
					 curslice.set(i, tmpcur);
				 }
			}
			result.set(x, curslice);
			formerxmax = curxmax;
		   }
		}
		
		//generate tiles info file for partitions
		int bufferindex = 0;
		for(int i=0;i<result.size();i++)
		{
			ArrayList<MBR> slice = result.get(i);
			for(int j=0;j<slice.size();j++)
			{
			    out.println((i*slice.size()+j)+"\t"+slice.get(j).toString("\t"));
			    wktout.println((i*slice.size()+j)+"\t"+slice.get(j).toString());
			    if(j!=slice.size()-1){
			    	wktbufferout.println((bufferindex)+"\t"+new MBR(slice.get(j).xmin,slice.get(j).xmax,slice.get(j).ymax-Variable.maxDistance*2,slice.get(j).ymax+Variable.maxDistance*2).toString());
			    	bufferout.println((bufferindex++)+"\t"+new MBR(slice.get(j).xmin,slice.get(j).xmax,slice.get(j).ymax-Variable.maxDistance*2,slice.get(j).ymax+Variable.maxDistance*2).toString("\t"));
			    }
			}
			double curx = slice.get(0).xmax;
			if(i!=result.size()-1){
				
				ArrayList<MBR> nextslice = result.get(i+1);
				ArrayList<Double> Ys = new ArrayList<Double>(); 
				Ys.add(slice.get(0).ymin);
				for(int j=0;j<slice.size()-1;j++){
					Ys.add(slice.get(j).ymax);
					Ys.add(nextslice.get(j).ymax);
				}
				Ys.add(slice.get(slice.size()-1).ymax);
				Collections.sort(Ys);
				for(int j=0;j<Ys.size()-1;j++){
					wktbufferout.println(bufferindex+"\t"+new MBR(curx-Variable.maxDistance*2,curx+Variable.maxDistance*2, Ys.get(j),Ys.get(j+1)).toString());
					bufferout.println((bufferindex++)+"\t"+new MBR(curx-Variable.maxDistance*2,curx+Variable.maxDistance*2, Ys.get(j),Ys.get(j+1)).toString("\t"));
					if(j!=0){
						wktbufferout.println(bufferindex+"\t"+new MBR(curx-Variable.maxDistance*2,curx+Variable.maxDistance*2,Ys.get(j)-Variable.maxDistance*2,Ys.get(j)+Variable.maxDistance*2).toString());
						bufferout.println((bufferindex++)+"\t"+new MBR(curx-Variable.maxDistance*2,curx+Variable.maxDistance*2,Ys.get(j)-Variable.maxDistance*2,Ys.get(j)+Variable.maxDistance*2).toString("\t"));
					}
				}
			}
			
		}

	}

	
	public static void main(String args[])
	{
		xDim = 2;
		yDim = 4;
		String sampleratestr;
		String geoindexstr;
		String input = null;
		String output;
		String bufferoutput;
		String threadnumstr;	
		Options options = new Options();
		
		Option help = new Option("h", "help", false, "display this help and exit.");
		help.setRequired(false);
		Option inputopts = new Option("i", "input", true, "input files, folder or file");
		inputopts.setRequired(true);
		inputopts.setArgName("input");
		Option outputopt = new Option("o","output",true,"output file for tiles. default is system out");
		outputopt.setRequired(false);
		outputopt.setArgName("output");
		Option bufferoutputopt = new Option("b","bufferoutput",true,"output file for buffer. default is system out");
		bufferoutputopt.setRequired(false);
		bufferoutputopt.setArgName("bufferoutput");
		Option threadnumopt = new Option("t","threadnum",true,"number of thread, default 2");
		threadnumopt.setRequired(false);
		threadnumopt.setArgName("threadnum");
		Option geoindexopt = new Option("g","geoindex",true,"column number of wkt, start from 0");
		geoindexopt.setRequired(false);
		geoindexopt.setArgName("geoindex");
		Option samplerateopt = new Option("s","samplerate",true,"sample rate, default 1");
		samplerateopt.setRequired(false);
		samplerateopt.setArgName("samplerate");
		Option completeopt = new Option("c","complete",false,"check this if you want to tile the whole domain instead of only the sample set");
		completeopt.setRequired(false);
		
		options.addOption(help);
		options.addOption(inputopts);
		options.addOption(outputopt);
		options.addOption(bufferoutputopt);
		options.addOption(threadnumopt);
		options.addOption(geoindexopt);
		options.addOption(samplerateopt);
		options.addOption(completeopt);
			
		CommandLineParser CLIparser = new GnuParser();
   		HelpFormatter formatter = new HelpFormatter();
   		CommandLine line = null;
   		try {
   			line = CLIparser.parse(options, args);
   			if(line.hasOption("h")) {
   				formatter.printHelp("tile generator", options, true);
   				System.exit(0);
   			}
   			geoindexstr = line.getOptionValue("g");
   	   		if(geoindexstr==null)
   	   		{
   				System.err.println("please specify the index of geometry in wkt format");
   				return;
   	   		}
   	   		geoindex = Integer.parseInt(geoindexstr);
   			
   			input = line.getOptionValue("i");
   			output = line.getOptionValue("o");
   			threadnumstr = line.getOptionValue("t");
   			sampleratestr = line.getOptionValue("s");
   			bufferoutput = line.getOptionValue("b");
   			if(output!=null)
   			try {
   				out = new PrintStream(new File(output));
   				wktout = new PrintStream(new File(output+".wkt"));
   			} catch (FileNotFoundException e) {
   				out = System.out;
   			}
   			
   			if(bufferoutput!=null){
   	   			try {
   	   				bufferout = new PrintStream(new File(bufferoutput));
   	   				wktbufferout = new PrintStream(new File(bufferoutput+".wkt"));
   	   			} catch (FileNotFoundException e) {
   	   				bufferout = System.out;
   	   			}
   			}
   			threadnum = threadnumstr==null?threadnum:Integer.parseInt(threadnumstr);
   			samplerate = sampleratestr==null?samplerate:Double.parseDouble(sampleratestr);
   			samplerate = samplerate>1||samplerate<=0?1:samplerate;
   			complete = line.hasOption("c");
   			
   		} catch(Exception e) {
   			formatter.printHelp("gentile", options, true);
   			System.exit(1);
   		}
		FileParser.initFileList(input);
		for(int i=0;i<threadnum;i++)
		{
		   new Thread(new Partition(i)).start();
		}
	}

	
}
