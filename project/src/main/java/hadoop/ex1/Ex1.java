package hadoop.ex1;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Ex1 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Instant start = Instant.now();


		// JOB 1
		
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Job1");
		job1.setJarByClass(Ex1.class);
		
		job1.setMapperClass(Job1Mapper.class);
		job1.setReducerClass(Job1Reducer.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

		job1.waitForCompletion(true);
		
		Instant finish = Instant.now();
		System.out.println("COMPUTING TIME: " + Duration.between(start, finish).toMillis());
	}
}
