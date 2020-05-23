package hadoop.ex2_basic;

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
 * Ex2 BigData Project
 * 
 */
public class Ex2 extends Configured implements Tool{
	public int run(String[] args) throws Exception {

		/*PATHS*/
		Path temp = new Path("temp/ex2Job1Output");
		Path inputHSP = new Path(args[0]);
		Path inputHS = new Path(args[1]);
		Path output = new Path(args[2]);

		Configuration conf = getConf();

		/*JOB JOIN*/
		Job join = Job.getInstance(conf, "join");
		join.setJarByClass(Ex2.class);

		MultipleInputs.addInputPath(join, inputHSP,TextInputFormat.class, Ex2HSPMapper.class);
		MultipleInputs.addInputPath(join, inputHS,TextInputFormat.class, Ex2HSMapper.class);

		join.setReducerClass(Ex2JoinReducer.class);
		join.setOutputKeyClass(Text.class);
		join.setOutputValueClass(Text.class);
		join.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(join, temp);

		boolean succ = join.waitForCompletion(true);
		if (! succ) {
			System.out.println("Join failed, exiting");
			return -1;
		}


		/*JOB2*/
		Job job2 = Job.getInstance(conf, "job2");
		job2.setJarByClass(Ex2.class);

		FileInputFormat.setInputPaths(job2, temp);
		FileOutputFormat.setOutputPath(job2, output);

		job2.setMapperClass(Ex2Mapper.class);
		job2.setReducerClass(Ex2Reducer.class);

		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		succ = job2.waitForCompletion(true);
		if (! succ) {
			System.out.println("Job2 failed, exiting");
			return -1;
		}		
		return 0;
	}



	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: Job2 .../historical_stock_prices.csv .../historical_stocks.csv .../RISULTATO_JOB2");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new Ex2(), args);
		System.exit(res);
	}
}

