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

public class PlayerOfTheSeason {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration cobj = new Configuration();
		Job jobj = Job.getInstance(cobj," ");
		jobj.setJarByClass(PlayerOfTheSeason.class);
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
		String player = valueArr[13];
		String  p_1 = player + "," + 1;
		contx.write(new Text(season), new Text(p_1));
	}
}

public static class MyReducer extends Reducer<Text,Text,Text,Text>
{
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		TreeMap<Integer,String> tmap = new TreeMap<>();
		HashMap<String,Integer> hmap = new HashMap<>();
		int count = 0;
		for(Text val : values)
		{
			String valArr[] = val.toString().split(",");
			String player = valArr[0];
			count = Integer.parseInt(valArr[1]);
			if(hmap.containsKey(player))
			{
				int currentCount = hmap.get(player);
				hmap.put(player, currentCount+count);
			}
			else
			{
				hmap.put(player, count);
			}
		}
		for(Map.Entry<String,Integer> entry : hmap.entrySet())
		{
			String player = entry.getKey();
			int playerCount = entry.getValue();
			tmap.put(playerCount,player);
			if(tmap.size() > 1)
			{
				tmap.remove(tmap.firstKey());
			}
		}
		context.write(key, new Text(tmap.values().toString()));
	}
}
}
