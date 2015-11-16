package utility;

import java.util.ArrayList;
/**/
public class MyList extends ArrayList<Point>
{
	private static final long serialVersionUID = 1L;
	
	public MyList mergeInOrder(MyList l2)
	{
		MyList l1 = this;
		MyList tmp = new MyList();
		int index1 = 0;
		int index2 = 0;
        int size1 = l1.size();
        int size2 = l2.size();
        //System.out.println(size1+" "+size2 );
		while(true) 
		{
			if(index1==size1&&index2==size2)
			{
				break;
			}
			else if(index2==size2||(index1<size1&&l1.get(index1).getX()<=l2.get(index2).getX()))
	        {
	        	tmp.add(l1.get(index1++));
	        }
	        else
	        {
	        	tmp.add(l2.get(index2++));
	        }
	    }
		return tmp;
	}
	
}