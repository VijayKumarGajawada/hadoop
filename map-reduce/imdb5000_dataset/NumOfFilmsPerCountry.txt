package mapreduce.imdb5000.progs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NumOfFilmsPerCountry {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration cobj = new Configuration();
		Job jobj = Job.getInstance(cobj," ");
		jobj.setJarByClass(NumOfFilmsPerCountry.class);
		jobj.setMapperClass(MyMapper.class);
		jobj.setReducerClass(MyReducer.class);
		jobj.setMapOutputKeyClass(Text.class);
		jobj.setMapOutputValueClass(Text.class);
		jobj.setOutputKeyClass(Text.class);
		jobj.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(jobj, new Path(args[0]));
		FileSystem.get(cobj).delete(new Path(args[1]), true);
		FileOutputFormat.setOutputPath(jobj, new Path(args[1]));
		System.exit(jobj.waitForCompletion(true) ? 0 : 1);
	}

public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>
{
	public void map(LongWritable key, Text value, Context contx) throws IOException, InterruptedException
	{
		String strValue = value.toString();
		String[] valueArr = strValue.trim().split(",");
		//String yearOfRelease = valueArr[1];
		String movieTitle = valueArr[3].trim();
		String country = valueArr[24];
		contx.write(new Text(country), new Text(movieTitle));
	}
}

public static class MyReducer extends Reducer<Text,Text,Text,IntWritable>
{
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		int movieCount = 0;
		for(Text val : values)
		{
			movieCount++;
		}
		context.write(key,new IntWritable(movieCount));
	}
}
}
