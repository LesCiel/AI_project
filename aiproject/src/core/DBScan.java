package core;

import java.util.ArrayList;
import java.lang.Math;
import java.io.*;

import utility.Point;
import utility.Variable;

public class DBScan {

	private final int minPts;
	private double eps;
	public static String inputfile = "";
	public DBScan(double eps, int minPts){
		
		this.eps=eps;
		this.minPts=minPts;
		
	}

    public ArrayList<ArrayList<Point>> get_clusters(ArrayList<Point> points){
    	System.out.println("clustering "+points.size()+" points");
    	ArrayList<ArrayList<Point>> clusters= new ArrayList<ArrayList<Point>>();
    	int index = 0;
    	int interval = points.size()/10;
    	int nextreport = interval;
    	for(Point point:points){
    		if(index++>=nextreport){
    			System.out.println(index*100/points.size()+"% done");
    			nextreport += interval;
    		}
    		ArrayList<Point> neighbors = getNeighbors(point,points);
    		// Type: 0 means noise, 1 means:core, 2 means border
    		if(neighbors.size()>minPts){
    			if(point.getType()==0||point.getType()==2){
    				point.setType(1);
    			}

    			for(Point item: neighbors){
    				if(item.getType()==0){
    					item.setType(2);;
    				}
    			}
    			ArrayList<Point> new_cluster= new ArrayList<Point>();
    			new_cluster.addAll(neighbors);
    			
    			for(Point item: neighbors){
    				if(item.getType()==1){
    					int index_value = -1; 
    					for(int i=0;i<clusters.size()&&index_value<0;i++){
    						ArrayList<Point> item_array = clusters.get(i);
    						for(Point p:item_array){
    							if(p.equal(item)){
    								index_value = i;
    								break;
    							}
    						}	
    					}
    					
    					if(index_value>=0){
    						new_cluster= merge(clusters.get(index_value),new_cluster);
    						clusters.remove(index_value);
    					}
    				}
    			}
    			clusters.add(new_cluster);
    			
    		}
    		
    	}

    	return clusters;
    }
	 private ArrayList<Point> merge(final ArrayList<Point> one, final ArrayList<Point> two) {
	        ArrayList<Point> result = new ArrayList<Point>();
	        result.addAll(one);
	        for(Point p2:two){
	        	boolean notcontain = true;
	        	for(Point p1:one){
	        		if(p1.equal(p2)){
	        			notcontain = false;
	        			break;
	        		}
	        	}
	        	if(notcontain){
	        		result.add(p2);
	        	}
	        }
	        return result;
	 }
	    
	 private ArrayList<Point> getNeighbors(final Point point, final ArrayList<Point> points) {
		 
	        final ArrayList<Point> neighbors = new ArrayList<Point>();
	        double distance;
	        for (final Point candidate : points) {
	        	distance = distance_compute(point,candidate);
	            if ( distance<=eps && distance>0) {
	                neighbors.add(candidate);
	            }
	        }
	        return neighbors;
	 }
	 private double distance_compute(Point p1, Point p2){
		 return Math.sqrt(Math.pow(p1.getX()-p2.getX(), 2)+Math.pow(p1.getY()-p2.getY(),2));
	 }
	 
	 public static ArrayList<Point> readFile(){
			ArrayList<Point> points=new ArrayList<Point>();
			try{		     
			    BufferedReader br = new BufferedReader(new FileReader(inputfile));
			    String strLine;  
			    while ((strLine = br.readLine()) != null)   {
                   String[] tokens = strLine.split("\t");
                   points.add(new Point(Double.parseDouble(tokens[0]),Double.parseDouble(tokens[1])));

			   }
			   br.close();
			}catch (Exception e){
				e.printStackTrace();
			     System.err.println("Error: " + e.getMessage());
			}
			System.out.println("read "+points.size()+" points");
			return points;
	 }
	 public static void main(String[] args) throws FileNotFoundException{
		 inputfile = "/ram/tweets_100000_uniq.csv";
		 ArrayList<Point> points= readFile();
		 
		 DBScan db=new DBScan(Variable.maxDistance,Variable.minPoints);  //esp, minpts, data, partitonid
		 ArrayList<ArrayList<Point>> clusters = db.get_clusters(points);
		 System.out.println("entirely "+clusters.size());
		 int index = 0;
		 String prefix = "/ram/output/dbscanoutput";
		 
		 for(ArrayList<Point> cluster:clusters){
			// if(cluster.size()<10)continue;
			 PrintStream ps = new PrintStream(new File(prefix+(index++)));
			 for(Point p:cluster){
				 ps.println(p.getX()+"\t"+p.getY());
			 }
			 System.out.println(cluster.size());
		 }
		 
	 }

}

