# assignment6.6
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TestJobCounters.NewSummer;
import org.apache.hadoop.mapreduce.*; 

public class assignment6_6map extends Mapper<Text, IntWritable, Text, IntWritable> {
	
	public void map(Text key, IntWritable value, Context context) 
			throws IOException, InterruptedException {
		
		context.write(key, value);
		
	}
}



import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class assignment6_6main {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		Job job = new Job(config, "sequence");
		job.setJarByClass(assignment6_6main.class);
		

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(assignment6_6map.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(job,new Path( args[1]));
		
		job.waitForCompletion(true);
		
		
		
		
	}
}
