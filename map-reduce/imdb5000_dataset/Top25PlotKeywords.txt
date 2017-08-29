package mapreduce.imdb5000.progs;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top25PlotKeywords {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration cobj = new Configuration();
		Job jobj = Job.getInstance(cobj," ");
		jobj.setJarByClass(Top25PlotKeywords.class);
		jobj.setMapperClass(MyMapper.class);
		jobj.setReducerClass(MyReducer.class);
		jobj.setMapOutputKeyClass(Text.class);
		jobj.setMapOutputValueClass(IntWritable.class);
		jobj.setOutputKeyClass(NullWritable.class);
		jobj.setOutputValueClass(Text.class);
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
		String[] valueArr = strValue.trim().split(",");
		String plotKeywords = valueArr[22];
		if(!plotKeywords.matches(""))
		{
			String keywords[] = plotKeywords.split("\\|");
			for(String word : keywords)
			{
				contx.write(new Text(word), new IntWritable(1));
			}
		}
	}
}

public static class MyReducer extends Reducer<Text,IntWritable,NullWritable,Text>
{
	TreeMap<Integer,String> tmap = new TreeMap<>();
	public void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		int wordCount = 0;
		for(IntWritable val : values)
		{
			wordCount+=val.get();
		}
		tmap.put(wordCount,key.toString());
	}
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		while(tmap.size()>25)
		{
			tmap.remove(tmap.firstKey());
		}
		context.write(NullWritable.get(),new Text(tmap.descendingMap().toString()));
	}
}
}
