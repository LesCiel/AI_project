package core;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

import utility.Point;

public class Component {
	
	
	public static ArrayList<ArrayList<String>> getComponents(ArrayList<String> tuples){
		
		HashMap<String, Boolean> universe = new HashMap<String,Boolean>();
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
		HashMap<String,Boolean> dedup = new HashMap<String, Boolean>();
		for(String s:tuples){
			dedup.put(s, Boolean.TRUE);
		}
		
		//retrieve the edge information
		HashMap<String,HashMap<String,Boolean>> neighbor = new HashMap<String, HashMap<String,Boolean>>();
		String tuple[];
		HashMap<String,Boolean> left;
		HashMap<String,Boolean> right;
		for(String v:dedup.keySet()){

			tuple = v.split("\t");
			universe.put(tuple[0], Boolean.TRUE);
			universe.put(tuple[1], Boolean.TRUE);
			
			left = neighbor.get(tuple[0]);
			right = neighbor.get(tuple[1]);
			if(left==null){
				left = new HashMap<String,Boolean>();
			}
			if(right==null){
				right = new HashMap<String,Boolean>();
			}
			left.put(tuple[1], Boolean.TRUE);
			right.put(tuple[0], Boolean.TRUE);
			neighbor.put(tuple[0], left);
			neighbor.put(tuple[1], right);
		}
		
		HashMap<String,Boolean> curcomponent_map = new HashMap<String,Boolean>();
		HashMap<String,Boolean> newadded = new HashMap<String,Boolean>();
		while(!universe.isEmpty()){
			String p = universe.keySet().iterator().next();
			universe.remove(p);
			curcomponent_map.clear();
			newadded.clear();
			newadded.put(p, Boolean.TRUE);
			curcomponent_map.put(p, Boolean.TRUE);
			
			while(!newadded.isEmpty()){
				ArrayList<String> newpoint = new ArrayList<String>();
				for(String s:newadded.keySet()){
					newpoint.add(s);
				}
				newadded.clear();
				for(String s:newpoint){
					HashMap<String,Boolean> neighbors = neighbor.get(s);
					for(String n:neighbors.keySet()){
						if(!curcomponent_map.containsKey(n)){
							curcomponent_map.put(n, Boolean.TRUE);
							newadded.put(n, Boolean.TRUE);
							universe.remove(n);
						}
					}
				}
				
			}
			ArrayList<String> curcomponent = new ArrayList<String>();
			for(String s:curcomponent_map.keySet()){
				curcomponent.add(s);
			}
			result.add(curcomponent);
		}
		
		
		
		return result;
		
	}
	public static ArrayList<String> readFile(String inputfile){
		ArrayList<String> points=new ArrayList<String>();
		try{		     
		    BufferedReader br = new BufferedReader(new FileReader(inputfile));
		    String strLine;  
		    while ((strLine = br.readLine()) != null)   {
               points.add(strLine);
		   }
		   br.close();
		}catch (Exception e){
			e.printStackTrace();
		     System.err.println("Error: " + e.getMessage());
		}
		return points;
 }
	public static void main(String args[]){
		
		
		
		ArrayList<String> vertices = readFile("/ram/clustermap/total");
		ArrayList<ArrayList<String>> result = getComponents(vertices);
		
		for(ArrayList<String> c:result){
			for(String s:c){
				System.out.print(s+"\t");
			}
			System.out.println();
		}
		
	}

}
