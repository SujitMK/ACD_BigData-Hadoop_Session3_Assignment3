package session3Assignment3_2PKG;

import org.apache.hadoop.io.*;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

public class TVSetMapper3_2 extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
	{
		
		String strArray[] = values.toString().split("\\|");
		
		if(strArray[0].equals("NA") || strArray[1].equals("NA")){
			
			// Do Nothing
		
		}
		else
		{
			
			
			context.write(new Text(strArray[0]), new IntWritable(1));
	       		
		}
	}

}
