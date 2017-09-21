//5) Find the most popular top 10 job positions for H1B visa applications for each year?
//a) for all the applications

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top10Jobs_PerYear_All {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration cobj = new Configuration();
		Job jobj = Job.getInstance(cobj," ");
		jobj.setJarByClass(Top10Jobs_PerYear_All.class);
		jobj.setMapperClass(MyMapper.class);
		jobj.setReducerClass(MyReducer.class);
		jobj.setMapOutputKeyClass(Text.class);
		jobj.setMapOutputValueClass(Text.class);
		jobj.setOutputKeyClass(Text.class);
		jobj.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobj, new Path(args[0]));
		FileSystem.get(cobj).delete(new Path(args[1]), true);
		FileOutputFormat.setOutputPath(jobj, new Path(args[1]));
		System.exit(jobj.waitForCompletion(true) ? 0 : 1);
	}

public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String strValue = value.toString();
		String[] valueArr = strValue.split("\t");
		String job_title = valueArr[4];
		String year = valueArr[7];
		String job_1 = job_title + "%%" + 1;
		context.write(new Text(year), new Text(job_1));
	}
}

public static class MyReducer extends Reducer<Text,Text,Text,Text>
{
	HashMap<String,Integer> hMap = new HashMap<>();
	TreeMap<Integer,String> tMap = new TreeMap<>();
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		for(Text val : values)
		{
			String valArr[] = val.toString().split("%%");
			String job_title = valArr[0];
			int count = Integer.parseInt(valArr[1]);
			if(hMap.containsKey(job_title))
			{
				int currentCount = hMap.get(job_title);
				hMap.put(job_title, currentCount+count);
			}
			else
			{
				hMap.put(job_title, count);
			}
		}
		
		for(Map.Entry<String,Integer> entry : hMap.entrySet())
		{
			String job_title = entry.getKey();
			int count = entry.getValue();
			tMap.put(count,job_title);
			if(tMap.size() > 10)
			{
				tMap.remove(tMap.firstKey());
			}
		}
		context.write(key, new Text(tMap.descendingMap().toString()));
	}
}
}
