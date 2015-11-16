package utility;

public class Point {
	double X;
	double Y;
	int type = 0;
	String id;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public Point(double X, double Y)
	{
		this.X = X;
		this.Y = Y;
		
	}
	public Point(double X, double Y, int type)
	{
		this.X = X;
		this.Y = Y;
		this.type = type;
		
	}
	public double getX() {
		return X;
	}
	public void setX(double x) {
		X = x;
	}
	public double getY() {
		return Y;
	}
	public void setY(double y) {
		Y = y;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	
	public boolean equal(Point another){
		return another.X == this.X && another.Y ==this.Y;
	}
	
	

}
