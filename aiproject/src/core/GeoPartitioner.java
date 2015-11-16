package core;

import java.util.ArrayList;
import java.util.List;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.WKTReader;

/**
 * this class maintain an Rtree of tiles, and get the join result of each query
 * */
public class GeoPartitioner{
    STRtree rtree;
    WKTReader wktreader = new WKTReader(new GeometryFactory());

    /**
     *read boundary file which is used to partition the input geometries 
     **/
    public GeoPartitioner(ArrayList<String> tiles)
    {

    	 String tokens[] = null;
		 rtree = new STRtree();
		 for(String tile:tiles)
		 {
			tokens = tile.split("\t");
			rtree.insert(new Envelope(Double.parseDouble(tokens[1]),Double.parseDouble(tokens[2]),Double.parseDouble(tokens[3]),Double.parseDouble(tokens[4])), tokens[0]);
		 }

    }
   
	public String[] assignKey(double x, double y)
	{

		String [] result=null; //one point or poly could belong to multiple tiles
		int i=0;
		try {
			Envelope envelope = new Envelope();
			envelope.init(x, x, y, y);
			List<?> list = rtree.query(envelope);
			if(list==null||list.size()<=0)
				return null;
			result = new String[list.size()];
			//System.err.println(result.length);
			for(Object obj:list)
			{
				result[i++] = (String)obj;
			}
				
		} catch (Exception e) {
			//e.printStackTrace(System.err);
			return null;
		}
		return result;
		
	}
}
