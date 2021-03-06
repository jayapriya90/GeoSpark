package org.datasyslab.geospark.partition;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

import scala.Tuple2;

public class PartitionAssignGridPoint implements PairFlatMapFunction<java.util.Iterator<Point>,Integer,Point>, Serializable  
{
	//This function is to assign grid index to each point in large dataset.
		int gridNumberHorizontal;
		int gridNumberVertical;
		Double[] gridHorizontalBorder;
		Double[] gridVerticalBorder;

		public PartitionAssignGridPoint(int gridNumberHorizontal, int gridNumberVertical, Double[] gridHorizontalBorder, Double[] gridVerticalBorder) {
			this.gridNumberHorizontal=gridNumberHorizontal;
			this.gridNumberVertical=gridNumberVertical;
			this.gridHorizontalBorder=gridHorizontalBorder;
			this.gridVerticalBorder=gridVerticalBorder;
		}

		public Iterable<Tuple2<Integer, Point>> call(Iterator<Point> s) throws Exception 	
		{
			//int id=-1;
			ArrayList<Tuple2<Integer, Point>> list=new ArrayList<Tuple2<Integer, Point>>();
			
			while(s.hasNext())
					{
					Point currentElement=s.next();
					Integer id=0;
					for(int j=0;j<gridNumberVertical;j++)
					{
						for(int i=0;i<gridNumberHorizontal;i++)
						{
							Envelope currentGrid=new Envelope(gridHorizontalBorder[i],gridHorizontalBorder[i+1],gridVerticalBorder[j],gridVerticalBorder[j+1]);	
							/*if(currentElement.getX()>=gridHorizontalBorder[i] && currentElement.getX()<=gridHorizontalBorder[i+1] && currentElement.getY()>=gridVerticalBorder[j] && currentElement.getY()<=gridVerticalBorder[j+1])
								{
									id=i*gridNumberHorizontal+j;
									list.add(new Tuple2(id,currentElement));
								}*/
							if(currentGrid.intersects(currentElement.getCoordinate())||currentGrid.contains(currentElement.getCoordinate()))
							{
								//id=j*gridNumberHorizontal+i;
								list.add(new Tuple2(id,currentElement));
							}
							id++;
							}
						}
					}
					
			
			return list;
		}
}
