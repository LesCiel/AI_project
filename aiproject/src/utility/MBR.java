package utility;

import com.vividsolutions.jts.geom.Point;

public class MBR {
	
	public double xmin;
	public double xmax;
	public double ymin;
	public double ymax;
	public Object index;
	
	public MBR(double xmin,double xmax,double ymin,double ymax)
	{
		this.xmin = xmin;
		this.xmax = xmax;
		this.ymin = ymin;
		this.ymax = ymax;
	}
	
	//adjust the mbr size by new point
	public void adjust(Point point)
	{
		this.xmin = Math.min(this.xmin, point.getX());
		this.xmax = Math.max(this.xmax, point.getX());
		this.ymin = Math.min(this.ymin, point.getY());
		this.ymax = Math.max(this.ymax, point.getY());
	}
	
	public void adjust(MBR mbr)
	{
		this.xmin = Math.min(this.xmin, mbr.xmin);
		this.xmax = Math.max(this.xmax, mbr.xmax);
		this.ymin = Math.min(this.ymin, mbr.ymin);
		this.ymax = Math.max(this.ymax, mbr.ymax);
	}
	public String toString()
    {
    	return "POLYGON((" + xmin + " " + ymin + ", "
    			+ xmin + " " + ymax + ", "
    			+ xmax + " " + ymax + ", "
    			+ xmax + " " + ymin + ", "
    			+ xmin + " " + ymin + "))";
    }
	
	public String toString(String del){
		return xmin+del+xmax+del+ymin+del+ymax;
	}

}
