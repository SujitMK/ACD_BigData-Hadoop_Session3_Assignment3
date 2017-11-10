package session3Assignment3_3PKG;

import org.apache.hadoop.io.*;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TVSetMapper3_3 extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
	{
		
		String strArray[] = values.toString().split("\\|");
		
		if(strArray[0].equals("Onida"))
		{
			
		   context.write(new Text(strArray[3]), new IntWritable(1));
	       		
		}
	}

}
