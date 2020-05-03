package hadoop.job2;

import java.time.Duration;
import java.time.Instant;

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


public class Job2 extends Configured implements Tool{
	public int run(String[] args) throws Exception {
		
		Instant start = Instant.now();
		
		/*PATHS*/
		Path inputHS = new Path(args[0]);
		Path inputHSP = new Path(args[1]);
		Path temp = new Path("temp");
		Path output = new Path(args[2]);
		
		Configuration conf = getConf();
		
		/*JOIN*/
		@SuppressWarnings("deprecation")
		Job join = new Job(conf, "join");
		join.setJarByClass(Job2.class);

		MultipleInputs.addInputPath(join, inputHS,TextInputFormat.class, JoinHistoricalStocksMapper.class);
		MultipleInputs.addInputPath(join, inputHSP,TextInputFormat.class, JoinHistoricalStockPricesMapper.class);
		FileOutputFormat.setOutputPath(join, temp);
		
		join.setReducerClass(JoinReducer.class);
		join.setOutputKeyClass(Text.class);
		join.setOutputValueClass(Text.class);
		join.setOutputFormatClass(TextOutputFormat.class);

		boolean succ = join.waitForCompletion(true);
		
		if (! succ) {
			System.out.println("Join failed, exiting");
			return -1;
		}
		/*JOB2*/
		@SuppressWarnings("deprecation")
		Job job2 = new Job(conf, "job2");
		job2.setJarByClass(Job2.class);
		
		FileInputFormat.setInputPaths(job2, temp);
		FileOutputFormat.setOutputPath(job2, output);
		
		job2.setMapperClass(Job2Mapper.class);
		job2.setReducerClass(Job2Reducer.class);
		
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
		
		Instant finish = Instant.now();
		System.out.println("COMPUTING TIME: " + Duration.between(start, finish));
		
		return 0;
	}
	
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: Job2 .../historical_stocks.csv .../historical_stock_prices.csv .../RISULTATO_JOB2");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new Job2(), args);
		System.exit(res);
	}
}

