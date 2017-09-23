//4)Which top 5 employers file the most petitions each year? - Case Status – ALL

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

public class Top5Employers_PerYear {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration cobj = new Configuration();
		Job jobj = Job.getInstance(cobj," ");
		jobj.setJarByClass(Top5Employers_PerYear.class);
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
		String employer_name = valueArr[2];
		String year = valueArr[7];
		String emp_1 = employer_name + "%%" + 1;
		context.write(new Text(year), new Text(emp_1));
	}
}

public static class MyReducer extends Reducer<Text,Text,Text,Text>
{
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		HashMap<String,Integer> hMap = new HashMap<>();
		TreeMap<Integer,String> tMap = new TreeMap<>();
		for(Text val : values)
		{
			String valArr[] = val.toString().split("%%");
			String employer_name = valArr[0];
			int count = Integer.parseInt(valArr[1]);
			if(hMap.containsKey(employer_name))
			{
				int currentCount = hMap.get(employer_name);
				hMap.put(employer_name, currentCount+count);
			}
			else
			{
				hMap.put(employer_name, count);
			}
		}
		
		for(Map.Entry<String,Integer> entry : hMap.entrySet())
		{
			String employer_name = entry.getKey();
			int count = entry.getValue();
			tMap.put(count,employer_name);
			if(tMap.size() > 5)
			{
				tMap.remove(tMap.firstKey());
			}
		}
		context.write(key, new Text(tMap.descendingMap().toString()));
	}
}
}
