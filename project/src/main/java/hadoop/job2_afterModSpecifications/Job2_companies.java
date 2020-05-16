package hadoop.job2_afterModSpecifications;

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

import hadoop.job2.Job2;
import hadoop.job2.JoinHistoricalStockPricesMapper;
import hadoop.job2.JoinReducer;

public class Job2_companies extends Configured implements Tool{
	public int run(String[] args) throws Exception {
		
		Instant start = Instant.now();
		
		/*PATHS*/
		Path inputHS = new Path(args[0]);
		Path inputHSP = new Path(args[1]);
		Path temp1 = new Path("temp1");
		Path temp2 = new Path("temp2");
		Path output = new Path(args[2]);
		
		Configuration conf = getConf();
		
		/*JOIN*/
		@SuppressWarnings("deprecation")
		Job join = new Job(conf, "join");
		join.setJarByClass(Job2_companies.class);

		MultipleInputs.addInputPath(join, inputHS,TextInputFormat.class, JoinHistoricalStocksMapper_withCompany.class);
		MultipleInputs.addInputPath(join, inputHSP,TextInputFormat.class, JoinHistoricalStockPricesMapper.class);
		FileOutputFormat.setOutputPath(join, temp1);
		
		join.setReducerClass(JoinReducer.class);
		join.setOutputKeyClass(Text.class);
		join.setOutputValueClass(Text.class);
		join.setOutputFormatClass(TextOutputFormat.class);

		boolean succ = join.waitForCompletion(true);
		
		if (! succ) {
			System.out.println("Join failed, exiting");
			return -1;
		}
		
		
		/*JOB2 companies part*/
		@SuppressWarnings("deprecation")
		Job job2Companies = new Job(conf, "job2_companies");
		job2Companies.setJarByClass(Job2_companies.class);
		
		FileInputFormat.setInputPaths(job2Companies, temp1);
		FileOutputFormat.setOutputPath(job2Companies, temp2);
		
		job2Companies.setMapperClass(Job2MapperCompany_withCompany.class);
		job2Companies.setReducerClass(Job2ReducerCompany_withCompany.class);
		
		job2Companies.setInputFormatClass(KeyValueTextInputFormat.class);
		job2Companies.setMapOutputKeyClass(Text.class);
		job2Companies.setMapOutputValueClass(Text.class);
		job2Companies.setOutputKeyClass(Text.class);
		job2Companies.setOutputValueClass(Text.class);
		job2Companies.setOutputFormatClass(TextOutputFormat.class);
		
		succ = job2Companies.waitForCompletion(true);

		if (! succ) {
			System.out.println("Job2 companies aggregation failed, exiting");
			return -1;
		}

		/*JOB2 sectors part*/
		
		@SuppressWarnings("deprecation")
		Job job2Sectors = new Job(conf, "job2_companies");
		job2Sectors.setJarByClass(Job2_companies.class);
		
		FileInputFormat.setInputPaths(job2Sectors, temp2);
		FileOutputFormat.setOutputPath(job2Sectors, output);
		
		job2Sectors.setMapperClass(Job2MapperSector_withCompany.class);
		job2Sectors.setReducerClass(Job2ReducerSector_withCompany.class);
		
		job2Sectors.setInputFormatClass(KeyValueTextInputFormat.class);
		job2Sectors.setMapOutputKeyClass(Text.class);
		job2Sectors.setMapOutputValueClass(Text.class);
		job2Sectors.setOutputKeyClass(Text.class);
		job2Sectors.setOutputValueClass(Text.class);
		job2Sectors.setOutputFormatClass(TextOutputFormat.class);
		
		succ = job2Sectors.waitForCompletion(true);

		if (! succ) {
			System.out.println("Job2 sectors aggregation failed, exiting");
			return -1;
		}
		
		
		Instant finish = Instant.now();
		System.out.println("COMPUTING TIME: " + Duration.between(start, finish).toMillis());
		
		return 0;
	}
	
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: Job2_companies .../historical_stocks.csv .../historical_stock_prices.csv .../RISULTATO_JOB2");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new Job2(), args);
		System.exit(res);
	}
}
