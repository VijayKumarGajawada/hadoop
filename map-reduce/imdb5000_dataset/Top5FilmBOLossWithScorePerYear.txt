package mapreduce.imdb5000.progs;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top5FilmScoreBOLossPerYear {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration cobj = new Configuration();
		Job jobj = Job.getInstance(cobj," ");
		jobj.setJarByClass(Top5FilmScoreBOLossPerYear.class);
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
		String[] valueArr = strValue.trim().split(",");
		if(valueArr[23].matches("English") & !valueArr[24].matches("India"))
		{
			if(!valueArr[18].matches("") & !valueArr[19].matches(""))
			{
				int budget = Integer.parseInt(valueArr[18]);
				int gross = Integer.parseInt(valueArr[19]);
				int loss = 0;
				String lang = valueArr[23];
				if(budget > gross)
				{
					loss = budget - gross;
					String yearOfRelease = valueArr[1];
					String movieTitle = valueArr[3].trim();
					String score = valueArr[2];
					String m_s_l = movieTitle + "//" + score + "//" + loss;
					context.write(new Text(yearOfRelease), new Text(m_s_l));
				}
			}
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
			String score = valArr[1];
			String movie_score = movie + "--" + score;
			int loss = Integer.parseInt(valArr[2]);
			tmap.put(loss,movie_score);
		}
		while(tmap.size()>5)
		{
			tmap.remove(tmap.firstKey());
		}
		context.write(key,new Text(tmap.descendingMap().toString()));
	}
}
}
