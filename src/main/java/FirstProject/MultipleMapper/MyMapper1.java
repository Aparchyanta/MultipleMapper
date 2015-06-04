package FirstProject.MultipleMapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper1 extends Mapper<LongWritable, Text, Text, Text>{
	protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,Text>.Context context) 
			throws java.io.IOException ,InterruptedException {
		String line=value.toString();
		String[] arr=line.split(",");
		String id=arr[0];
		String location=arr[2];
		String name=arr[1];
		if(location.equals("CA")){
			context.write(new Text(id.concat(name)), new Text(location));
		}
		
	}

}
