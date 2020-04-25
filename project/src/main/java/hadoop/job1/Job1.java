package hadoop.job1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job1 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		@SuppressWarnings("deprecation")
		Job job = new Job(new Configuration(), "Job1");
		job.setJarByClass(Job1.class);
		
		job.setMapperClass(Job1Mapper.class);
		job.setReducerClass(Job1Reducer.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);					//maybe to change, look down
		job.setOutputValueClass(Job1Result.class);			//maybe to change, look down
		/*Calling job.setOutputKeyClass( NullWritable.class ); will set the types expected as output from both the map and reduce phases.
		If your Mapper emits different types than the Reducer, you can set the types emitted by the mapper with the JobConf's setMapOutputKeyClass() 
		and setMapOutputValueClass() methods. These implicitly set the input types expected by the Reducer.*/

		job.waitForCompletion(true);
	}
}
