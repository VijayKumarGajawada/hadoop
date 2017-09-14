package mapreduce.imdb5000.progs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgScorePerYear {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration cobj = new Configuration();
		Job jobj = Job.getInstance(cobj," ");
		jobj.setJarByClass(AvgScorePerYear.class);
		jobj.setMapperClass(MyMapper.class);
		jobj.setReducerClass(MyReducer.class);
		jobj.setMapOutputKeyClass(Text.class);
		jobj.setMapOutputValueClass(FloatWritable.class);
		jobj.setOutputKeyClass(Text.class);
		jobj.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(jobj, new Path(args[0]));
		FileSystem.get(cobj).delete(new Path(args[1]), true);
		FileOutputFormat.setOutputPath(jobj, new Path(args[1]));
		System.exit(jobj.waitForCompletion(true) ? 0 : 1);
	}

public static class MyMapper extends Mapper<LongWritable,Text,Text,FloatWritable>
{
	public void map(LongWritable key, Text value, Context contx) throws IOException, InterruptedException
	{
		String strValue = value.toString();
		String[] valueArr = strValue.split(",");
		String yearOfRelease = valueArr[1];
		float imdbScore = Float.parseFloat(valueArr[2]);
		contx.write(new Text(yearOfRelease), new FloatWritable(imdbScore));
	}
}

public static class MyReducer extends Reducer<Text,FloatWritable,Text,FloatWritable>
{
	public void reduce(Text key,Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException
	{
		float avgScore = 0.0f;
		float sumScore = 0.0f;
		int movieCount = 0;
		for(FloatWritable val : values)
		{
			sumScore += val.get();
			movieCount++;
		}
		avgScore = sumScore/(float)movieCount;
		context.write(key,new FloatWritable(avgScore));
	}
}
}
