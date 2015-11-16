package core;
            
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import utility.Point;
import utility.Variable;
         
public class Joiner{

    public static class Tiler extends Mapper<LongWritable, Text, Text, Text> 
    {
    	GeoPartitioner partitioner;
    	@Override
        public void setup(Context context)
        {

    		Configuration conf = context.getConfiguration();
    		String partitions[] = conf.get("partition").split("\n");
    		ArrayList<String> tmp = new ArrayList<String>();
    		for(String s:partitions){
    			tmp.add(s);
    		}
    		partitioner = new GeoPartitioner(tmp);  
        }
    	
       @Override
       public void map(LongWritable key, Text value, Context context) 
    		   throws IOException, InterruptedException 
       {

    	   String tokens[] = value.toString().split("\t");
    	   String partitions[] = partitioner.assignKey(Double.parseDouble(tokens[0]),Double.parseDouble(tokens[1]));
    	   if(partitions!=null){
    		   for(String p:partitions){
    	    	   context.write(new Text(p), value);
    		   }
    	   }
       }
    } 

    public static class LocalDBScan extends Reducer<Text, Text, NullWritable, Text>
    {
      DBScan dbscan = new DBScan(Variable.maxDistance,Variable.minPoints);

	  @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
    		  throws IOException, InterruptedException{
		  
		  ArrayList<Point> points = new ArrayList<Point>();
		  String partitionid = key.toString();
		  String tokens[];
		  for(Text value:values){
			  tokens = value.toString().split("\t");
			  points.add(new Point(Double.parseDouble(tokens[0]),Double.parseDouble(tokens[1])));
		  }
		  
		  ArrayList<ArrayList<Point>> clusters = dbscan.get_clusters(points);

		  for(int i=0;i<clusters.size();i++){
			  String clusterid = partitionid+"_"+i;
			  for(Point point:clusters.get(i)){
				 context.write(NullWritable.get(), new Text(point.getX()+"\t"+point.getY()+"\t"+clusterid)); 
			  }
		  }
    	  
      }//end reduce function
    }//end class
    
    public static class ClusterMerge extends Reducer<Text, Text, Text, NullWritable>
    {
      DBScan dbscan = new DBScan(Variable.maxDistance,Variable.minPoints);

	  @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
    		  throws IOException, InterruptedException{
		  
		  ArrayList<Point> points = new ArrayList<Point>();
		  String tokens[];
		  for(Text value:values){
			  tokens = value.toString().split("\t");
			  Point point = new Point(Double.parseDouble(tokens[0]),Double.parseDouble(tokens[1]));
			  point.setId(tokens[2]);
			  points.add(point);
		  }
		  
		  ArrayList<ArrayList<Point>> clusters = dbscan.get_clusters(points);
		  for(ArrayList<Point> cluster:clusters){
			  
			  HashMap<String, Boolean> map= new HashMap<String, Boolean>();
			  for(Point point:cluster){
				 map.put(point.getId(), Boolean.TRUE);
			  }
			  Set<String> ids = map.keySet();
			  for(String id1:ids){
				  for(String id2:ids){
					  if(!id1.equalsIgnoreCase(id2)){
						  if(id1.compareTo(id2)>0){
							  context.write(new Text(id1+"\t"+id2), NullWritable.get());
						  }else{
							  context.write(new Text(id2+"\t"+id1), NullWritable.get());
						  }
					  }
				  }
			  }
		  }
    	  
      }//end reduce function
    }//end class
    
    
    public static String read(String filepath){
    	String content = "";
    	try {
			BufferedReader reader = new BufferedReader(new FileReader(filepath));
			String line = "";
			content = reader.readLine();
			while((line=reader.readLine())!=null){
				content = content+"\n"+line;
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return content;
    	
    }
    public static void main(String[] args) throws Exception{

    	
    	String partitions = read(args[0]);
    	Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://perseus/");
        conf.set("partition", partitions);
        try{
 	       FileSystem fs = FileSystem.get(conf);
 	       fs.delete(new Path("/subclusters"),true);
 	       }catch(Exception e){}
 	
        
    	@SuppressWarnings("deprecation")
        Job job = new Job(conf, "first");
        job.setNumReduceTasks(4);
        job.setJarByClass(Joiner.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
            
        job.setMapperClass(Tiler.class);
        job.setReducerClass(LocalDBScan.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path("/small"));
        FileOutputFormat.setOutputPath(job, new Path("/subclusters"));
        job.submit();
        job.waitForCompletion(true);
        
        System.out.println("done with the first phase!");
        
        
        
        String buffer = read(args[1]);
    	Configuration confbuffer = new Configuration();
    	confbuffer.set("fs.defaultFS", "hdfs://perseus/");
    	confbuffer.set("partition", buffer);        
        try{
   	       FileSystem fs = FileSystem.get(confbuffer);
   	      fs.delete(new Path("/clustermap"),true);
   	       }catch(Exception e){}
        
    	@SuppressWarnings("deprecation")
        Job jobbuffer = new Job(confbuffer, "second");
    	jobbuffer.setNumReduceTasks(Integer.parseInt(args[2]));
    	jobbuffer.setJarByClass(Joiner.class);
        
    	jobbuffer.setOutputKeyClass(Text.class);
    	jobbuffer.setOutputValueClass(Text.class);
            
    	jobbuffer.setMapperClass(Tiler.class);
    	jobbuffer.setReducerClass(ClusterMerge.class);
        
    	jobbuffer.setInputFormatClass(TextInputFormat.class);
    	jobbuffer.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(jobbuffer, new Path("/subclusters"));
        FileOutputFormat.setOutputPath(jobbuffer, new Path("/clustermap"));
        
        jobbuffer.submit();
        jobbuffer.waitForCompletion(true);
        System.out.println("done with second phase");
    }
          
   }