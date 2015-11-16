package core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;

public abstract class FileParser implements Runnable{
	public static LinkedList<File> filelist = new LinkedList<File>();
	public static Object global = new Object();
	Object local;
    static Integer alive = 0;
    public static int threadnum = 5;
    public static int totalfile = -1;
    public static Integer lineCount = 0;
    protected long start = 0;
    protected int count = 0;
    protected int threadid = 0;
    
    protected void print(Object obj)
    {
    	System.out.println("Thread "+threadid+": "+obj.toString());
    }
    public FileParser(int id)
    {
    	synchronized(alive)
    	{
    		alive++;
    	}
    	threadid = id;
    	start = System.currentTimeMillis();
    }
	abstract protected void parseline(String filename, String line);
	abstract protected void reduce();
	protected void afterreduce(){};
	abstract protected void last();
	protected void parsefile(File file)
	{
		try {
			BufferedReader bf = new BufferedReader(new FileReader(file));
			String line;
			while((line=bf.readLine())!=null)
			{
				count++;
				parseline(file.getName(),line);
			}

			bf.close();
		}catch (IOException e) {
			e.printStackTrace();
		}
	}
	void afterfile(int filesize,File file)
	{
		synchronized(lineCount)
		{
			lineCount = lineCount + count;
			count = 0;
		}
	};
	@Override
	public void run() {
		
		while(true)
		{
			File file;
			int filesize = 0;
			synchronized(filelist)
			{
				filesize = filelist.size();
				if(totalfile==-1)
					totalfile = filesize;
				file = filelist.poll();
				
			}
			if(file==null)
				break;
			if(!file.exists())
				continue;
			parsefile(file);
			afterfile(filesize,file);

		}
		synchronized(global)
		{
			reduce();
		}
		afterreduce();
		synchronized(alive)
		{
			
			if(--alive == 0)
			{
				last();
			}
		}
	}
	
	public static void initFileList(String path)
	{
		File dir = new File(path);	   
	    if(dir.isDirectory())
	    for(File f:dir.listFiles())
	    {
	    	filelist.add(f);
	    }
	    else if(dir.isFile())
	    {
	    	filelist.add(dir);
	    }
	    else
	    {
	    	System.out.println("no file is added!");
	    }
	    threadnum = Math.min(threadnum, filelist.size());
	}

}
