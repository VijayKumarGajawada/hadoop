package mapred.ipldata.programs;

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

public class BestDefendingTeamSeason {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration cobj = new Configuration();
		Job jobj = Job.getInstance(cobj," ");
		jobj.setJarByClass(BestDefendingTeamSeason.class);
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
		String winner = valueArr[10];
		String defend = "";
		String result = valueArr[8];
		if(!result.matches("no result"))
		{
			if(valueArr[12].matches("0"))
			{
				defend = winner + "," + 1;
			}
			else
			{
				defend = "chase";
			}
			contx.write(new Text(season), new Text(defend));
		}	
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
			if(!val.toString().matches("chase"))
			{
				String valArr[] = val.toString().split(",");
				String team = valArr[0];
				int count = Integer.parseInt(valArr[1]);
				if(hMap.containsKey(team))
				{
					int currentCount = hMap.get(team);
					hMap.put(team, currentCount+count);
				}
				else
				{
					hMap.put(team, count);
				}
			}
		}
		
		for(Map.Entry<String,Integer> entry : hMap.entrySet())
		{
			String team = entry.getKey();
			int count = entry.getValue();
			tMap.put(count,team);
			if(tMap.size() > 1)
			{
				tMap.remove(tMap.firstKey());
			}
		}
		String result = "Best Defending team : "+tMap.toString();
		context.write(key, new Text(result));
	}
}
}
