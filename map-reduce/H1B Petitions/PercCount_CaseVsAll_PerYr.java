//6) Find the percentage and the count of each case status on total applications for each year. 
//Create a line graph depicting the pattern of All the cases over the period of time.

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

public class PercCount_CaseVsAll_PerYr {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration cobj = new Configuration();
		Job jobj = Job.getInstance(cobj," ");
		jobj.setJarByClass(PercCount_CaseVsAll_PerYr.class);
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
		String case_status = valueArr[1];
		String year = valueArr[7];
		String case_1 = case_status + "%%" + 1;
		context.write(new Text(year), new Text(case_1));
	}
}

public static class MyReducer extends Reducer<Text,Text,Text,Text>
{
	HashMap<String,String> hMap2 = new HashMap<>();
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		HashMap<String,Long> hMap = new HashMap<>();
		long allCount = 0;
		float percentage = 0.0f;
		for(Text val : values)
		{
			String valArr[] = val.toString().split("%%");
			String case_status = valArr[0];
			long count = Long.parseLong(valArr[1]);
			if(hMap.containsKey(case_status))
			{
				long currentCount = hMap.get(case_status);
				hMap.put(case_status, currentCount+count);
			}
			else
			{
				hMap.put(case_status, count);
			}
			allCount++;
		}
		
		for(Map.Entry<String, Long> entry : hMap.entrySet())
		{
			String case_status = entry.getKey();
			long caseCount = entry.getValue();
			percentage = ((float)caseCount/(float)allCount)*100;
			hMap2.put(case_status, String.valueOf(percentage) + "% -" + String.valueOf(caseCount));
		}
		context.write(key, new Text(hMap2.toString()));
	}
}
}
