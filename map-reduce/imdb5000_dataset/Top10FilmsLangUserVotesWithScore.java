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

public class Top10LangUserVote_vs_Score {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration cobj = new Configuration();
		Job jobj = Job.getInstance(cobj," ");
		jobj.setJarByClass(Top10LangUserVote_vs_Score.class);
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
		String[] valueArr = strValue.trim().split(",");
		int numOfVotedUsers = Integer.parseInt(valueArr[8]);
		String movieTitle = valueArr[3];
		String language = valueArr[23];
		String score = valueArr[2];
		String m_u_s = movieTitle + "//" + numOfVotedUsers + "//" + score;
		contx.write(new Text(language), new Text(m_u_s));
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
			int userVotes = Integer.parseInt(valArr[1]);
			String score = valArr[2];
			String m_s = movie + "--" + score;
			tmap.put(userVotes,m_s);
		}
		while(tmap.size()>10)
		{
			tmap.remove(tmap.firstKey());
		}
		context.write(key,new Text(tmap.descendingMap().toString()));
	}
}
}
