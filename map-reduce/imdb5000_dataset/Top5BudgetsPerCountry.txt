package mapreduce.imdb5000.progs;

import java.io.IOException;
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

public class Top5BudgetsPerCountry {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration cobj = new Configuration();
		Job jobj = Job.getInstance(cobj," ");
		jobj.setJarByClass(Top5BudgetsPerCountry.class);
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
		String strValue = value.toString();
		String[] valueArr = strValue.split(",");
		String movieTitle = valueArr[3];
		if(!valueArr[18].matches(""))
		{
			int budget = Integer.parseInt(valueArr[18]);
			String m_b = movieTitle + "//" + budget;
			String country = valueArr[24];
			contx.write(new Text(country), new Text(m_b));
		}
	}
}

public static class MyReducer extends Reducer<Text,Text,Text,Text>
{
	public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		TreeMap<Integer,String> tmap = new TreeMap<>();
		for(Text val : values)
		{
			String valArr[] = val.toString().split("//");
			String movie = valArr[0];
			int budget = Integer.parseInt(valArr[1]);
			tmap.put(budget, movie);
		}
		while(tmap.size()>5)
		{
			tmap.remove(tmap.firstKey());
		}
		context.write(key,new Text(tmap.toString()));
	}
}
}
