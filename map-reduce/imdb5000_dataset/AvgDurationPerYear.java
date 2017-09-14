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

public class AvgDurationPerYear {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration cobj = new Configuration();
		Job jobj = Job.getInstance(cobj," ");
		jobj.setJarByClass(AvgDurationPerYear.class);
		jobj.setMapperClass(MyMapper.class);
		jobj.setReducerClass(MyReducer.class);
		jobj.setMapOutputKeyClass(Text.class);
		jobj.setMapOutputValueClass(IntWritable.class);
		jobj.setOutputKeyClass(Text.class);
		jobj.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(jobj, new Path(args[0]));
		FileSystem.get(cobj).delete(new Path(args[1]), true);
		FileOutputFormat.setOutputPath(jobj, new Path(args[1]));
		System.exit(jobj.waitForCompletion(true) ? 0 : 1);
	}

public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>
{
	public void map(LongWritable key, Text value, Context contx) throws IOException, InterruptedException
	{
		String strValue = value.toString();
		String[] valueArr = strValue.split(",");
		String yearOfRelease = valueArr[1];
		if(!valueArr[5].matches("")) {
			int duration = Integer.parseInt(valueArr[5]);
			contx.write(new Text(yearOfRelease), new IntWritable(duration));
		}
	}
}

public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>
{
	public void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		int avgDuration = 0;
		int sumDuration = 0;
		int movieCount = 0;
		for(IntWritable val : values)
		{
			sumDuration += val.get();
			movieCount++;
		}
		avgDuration = sumDuration/movieCount;
		context.write(key,new IntWritable(avgDuration));
	}
}
}
