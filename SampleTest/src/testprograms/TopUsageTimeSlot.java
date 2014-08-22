package testprograms;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopUsageTimeSlot {
	
	public static void main(String[] args) throws Exception  {
		Configuration conf = new Configuration();
		String[] otherArgs;
		otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2) {
		System.err.println("Usage : TopUsageTimeSlot <inputfilename> <outputfilename>");
		System.exit(2);
		}
		Job job = Job.getInstance(conf);
		job.setJobName("Top TimeSlot Usage");
		job.setJarByClass(TopUsageTimeSlot.class);
		job.setMapperClass(TopUsageTimeSlotMapper.class);
		job.setReducerClass(TopUsageTimeSlotReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

		
	


