package hadoop.ex3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/**
 * 
 * Ex3 BigData Project
 * 
 */
public class Ex3 extends Configured implements Tool{

	public int run(String[] args) throws Exception {
				
		/*PATHS*/
		Path temp = new Path("temp/ex3Job1Output");
		Path inputHSP = new Path(args[0]);
		Path inputHS = new Path(args[1]);
		Path output = new Path(args[2]);

		Configuration conf = getConf();

		/*JOB JOIN*/
		Job join = Job.getInstance(conf, "join");
		join.setJarByClass(Ex3.class);

		MultipleInputs.addInputPath(join, inputHSP,TextInputFormat.class, Ex3HSPMapper.class);
		MultipleInputs.addInputPath(join, inputHS,TextInputFormat.class, Ex3HSMapper.class);
		FileOutputFormat.setOutputPath(join, temp);

		join.setReducerClass(Ex3JoinReducer.class);
		join.setOutputKeyClass(Text.class);
		join.setOutputValueClass(Text.class);
		join.setOutputFormatClass(TextOutputFormat.class);

		boolean succ = join.waitForCompletion(true);
		if (!succ) {
			System.out.println("Join failed, exiting");
			return -1;
		}


		/*JOB3 companies part*/
		Job job3 = Job.getInstance(conf, "job3");
		job3.setJarByClass(Ex3.class);
		
		FileInputFormat.setInputPaths(job3, temp);
		FileOutputFormat.setOutputPath(job3, output);
		
		job3.setMapperClass(Ex3Mapper.class);
		job3.setReducerClass(Ex3Reducer.class);
		
		job3.setInputFormatClass(KeyValueTextInputFormat.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setOutputFormatClass(TextOutputFormat.class);

		succ = job3.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job3 failed, exiting");
			return -1;
		}
				
		return 0;
	}


	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: Job3 .../historical_stock_prices.csv .../historical_stocks.csv .../RISULTATO_JOB3");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new Ex3(), args);
		System.exit(res);
	}
	

}
