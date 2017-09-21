//7) Create a bar graph to depict the number of applications for each year [All] 

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

public class Num_PetiEachYear {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration cobj = new Configuration();
		Job jobj = Job.getInstance(cobj," ");
		jobj.setJarByClass(Num_PetiEachYear.class);
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
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String strValue = value.toString();
		String[] valueArr = strValue.split("\t");
		String year = valueArr[7];
		context.write(new Text(year), new IntWritable(1));
	}
}

public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>
{
	public void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		int count = 0;
		for(IntWritable val : values)
		{
			count+=val.get();
		}
		context.write(new Text(key), new IntWritable(count));
	}
}
}
