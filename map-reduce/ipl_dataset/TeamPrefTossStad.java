package mapred.ipldata.programs;

import java.io.IOException;
import java.util.HashMap;

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

public class TeamPrefTossStad {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration cobj = new Configuration();
		Job jobj = Job.getInstance(cobj," ");
		jobj.setJarByClass(TeamPrefTossStad.class);
		jobj.setMapperClass(MyMapper.class);
		jobj.setReducerClass(MyReducer.class);
		jobj.setMapOutputKeyClass(Text.class);
		jobj.setMapOutputValueClass(Text.class);
		//jobj.setNumReduceTasks(0);
		jobj.setOutputKeyClass(Text.class);
		jobj.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobj, new Path(args[0]));
		FileSystem.get(cobj).delete(new Path(args[1]), true);
		FileOutputFormat.setOutputPath(jobj, new Path(args[1]));
		System.exit(jobj.waitForCompletion(true) ? 0 : 1);
	}

public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>
{
	public void map(LongWritable key, Text value, Context contx) throws IOException, InterruptedException
	{
		String[] valueArr = value.toString().split(",");
		String tossWinner = valueArr[6];
		String tossDecision = valueArr[7];
		String stadium = valueArr[14];
		String w_d = tossWinner + "--" + tossDecision + "," + 1;
		contx.write(new Text(stadium), new Text(w_d));
	}
}

public static class MyReducer extends Reducer<Text,Text,Text,Text>
{
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		HashMap<String,Integer> hMap = new HashMap<>();
		for(Text val : values)
		{
			String valueArr[] = val.toString().split(",");
			String w_d = valueArr[0];
			int count = Integer.parseInt(valueArr[1]);
			if(hMap.containsKey(w_d))
			{
				int currentCount = hMap.get(w_d);
				hMap.put(w_d, currentCount+count);
			}
			else
			{
				hMap.put(w_d, count);
			}
		}
		context.write(key, new Text(hMap.toString()));
	}
}
}
