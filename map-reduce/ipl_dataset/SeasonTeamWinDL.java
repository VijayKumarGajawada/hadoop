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

public class SeasonTeamWinDL {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration cobj = new Configuration();
		Job jobj = Job.getInstance(cobj," ");
		jobj.setJarByClass(SeasonTeamWinDL.class);
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
	public void map(LongWritable key, Text value, Context contx) throws IOException, InterruptedException
	{
		String strValue = value.toString();
		String[] valueArr = strValue.split(",");
		String season = valueArr[1];
		String dl = valueArr[9];
		String team = valueArr[10];
		if(dl.matches("1"))
		{
			contx.write(new Text(season), new Text(team));
		}
	}
}

public static class MyReducer extends Reducer<Text,Text,Text,Text>
{
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		HashMap<String,Integer> hmap = new HashMap<>();
		String team = "";
		for(Text val : values)
		{
			team = val.toString();
			if(hmap.containsKey(team))
			{
				int count = hmap.get(team);
				hmap.put(team, 1+count);
			}
			else
			{
				hmap.put(team, 1);
			}
		}
		context.write(key, new Text(hmap.toString()));
	}
}
}
