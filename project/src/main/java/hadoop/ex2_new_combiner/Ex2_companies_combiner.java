package hadoop.ex2_new_combiner;

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

import hadoop.ex2.Ex2;
import hadoop.ex2.JoinHistoricalStockPricesMapper;
import hadoop.ex2.JoinReducer;
import hadoop.ex2_new.Ex2MapperCompany_withCompany;
import hadoop.ex2_new.Ex2MapperSector_withCompany;
import hadoop.ex2_new.Ex2ReducerCompany_withCompany;
import hadoop.ex2_new.Ex2ReducerSector_withCompany;
import hadoop.ex2_new.JoinHistoricalStocksMapper_withCompany;
import hadoop.ex2_new.JoinReducer_withCompany;

public class Ex2_companies_combiner extends Configured implements Tool{
	public int run(String[] args) throws Exception {
				
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
		join.setJarByClass(Ex2_companies_combiner.class);

		MultipleInputs.addInputPath(join, inputHS,TextInputFormat.class, JoinHistoricalStocksMapper_withCompany.class);
		MultipleInputs.addInputPath(join, inputHSP,TextInputFormat.class, JoinHistoricalStockPricesMapper.class);
		FileOutputFormat.setOutputPath(join, temp1);
		
		join.setReducerClass(JoinReducer_withCompany.class);
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
		Job job2Companies = new Job(conf, "job2_companies_combiner");
		job2Companies.setJarByClass(Ex2_companies_combiner.class);
		
		FileInputFormat.setInputPaths(job2Companies, temp1);
		FileOutputFormat.setOutputPath(job2Companies, temp2);
		
		job2Companies.setMapperClass(Ex2MapperCompany_withCompany.class);
		job2Companies.setCombinerClass(Ex2CombinerCompany_withCompany.class);
		job2Companies.setReducerClass(Ex2ReducerCompany_withCompany.class);
		
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
		Job job2Sectors = new Job(conf, "job2_sector_combiner");
		job2Sectors.setJarByClass(Ex2_companies_combiner.class);
		
		FileInputFormat.setInputPaths(job2Sectors, temp2);
		FileOutputFormat.setOutputPath(job2Sectors, output);
		
		job2Sectors.setMapperClass(Ex2MapperSector_withCompany.class);
		job2Sectors.setCombinerClass(Ex2CombinerSector_withCompany.class);
		job2Sectors.setReducerClass(Ex2ReducerSector_withCompany.class);
		
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
		
				
		return 0;
	}
	
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: Job2_companies .../historical_stocks.csv .../historical_stock_prices.csv .../RISULTATO_JOB2");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new Ex2_companies_combiner(), args);
		System.exit(res);
	}
}
