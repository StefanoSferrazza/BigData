package hadoop.ex1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * 
 * Ex1 BigData Project
 * 
 */
public class Ex1 extends Configured implements Tool{

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		// JOB 1
		
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Job1");
		job1.setJarByClass(Ex1.class);
		
		job1.setMapperClass(Ex1Mapper.class);
		job1.setReducerClass(Ex1Reducer.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        boolean succ = job1.waitForCompletion(true);
		if (! succ) {
			System.out.println("Job1 failed, exiting");
			return -1;
		}
		return 0;
	}
	
	
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: Job1 .../historical_stock_prices.csv .../RISULTATO_JOB1");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new Ex1(), args);
		System.exit(res);
	}
}
