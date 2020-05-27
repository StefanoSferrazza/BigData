package hadoop.ex2_comps_combiner;

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

import hadoop.ex2_basic.Ex2HSPMapper;
import hadoop.ex2_comps.Ex2CompanyMapper_Companies;
import hadoop.ex2_comps.Ex2CompanyReducer_Companies;
import hadoop.ex2_comps.Ex2HSMapper_Companies;
import hadoop.ex2_comps.Ex2JoinReducer_Companies;
import hadoop.ex2_comps.Ex2SectorMapper_Companies;
import hadoop.ex2_comps.Ex2SectorReducer_Companies;

/**
 * 
 * Ex2 Companies with Combiner BigData Project
 * 
 */
public class Ex2_companies_combiner extends Configured implements Tool{
	public int run(String[] args) throws Exception {
				
		/*PATHS*/
		Path inputHSP = new Path(args[0]);
		Path inputHS = new Path(args[1]);
		Path temp1 = new Path("temp1");
		Path temp2 = new Path("temp2");
		Path output = new Path(args[2]);
		
		Configuration conf = getConf();
		
		/*JOIN*/
		Job join = Job.getInstance(conf, "join");
		join.setJarByClass(Ex2_companies_combiner.class);

		MultipleInputs.addInputPath(join, inputHSP,TextInputFormat.class, Ex2HSPMapper.class);
		MultipleInputs.addInputPath(join, inputHS,TextInputFormat.class, Ex2HSMapper_Companies.class);
		FileOutputFormat.setOutputPath(join, temp1);
		
		join.setReducerClass(Ex2JoinReducer_Companies.class);
		join.setOutputKeyClass(Text.class);
		join.setOutputValueClass(Text.class);
		join.setOutputFormatClass(TextOutputFormat.class);

		boolean succ = join.waitForCompletion(true);
		
		if (! succ) {
			System.out.println("Join failed, exiting");
			return -1;
		}
		
		
		/*JOB2 companies part*/
		Job job2Companies = Job.getInstance(conf, "job2_companies_combiner");
		job2Companies.setJarByClass(Ex2_companies_combiner.class);
		
		FileInputFormat.setInputPaths(job2Companies, temp1);
		FileOutputFormat.setOutputPath(job2Companies, temp2);
		
		job2Companies.setMapperClass(Ex2CompanyMapper_Companies.class);
		job2Companies.setCombinerClass(Ex2CombinerCompany_withCompany.class);
		job2Companies.setReducerClass(Ex2CompanyReducer_Companies.class);
		
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
		Job job2Sectors = Job.getInstance(conf, "job2_sector_combiner");
		job2Sectors.setJarByClass(Ex2_companies_combiner.class);
		
		FileInputFormat.setInputPaths(job2Sectors, temp2);
		FileOutputFormat.setOutputPath(job2Sectors, output);
		
		job2Sectors.setMapperClass(Ex2SectorMapper_Companies.class);
		job2Sectors.setCombinerClass(Ex2CombinerSector_withCompany.class);
		job2Sectors.setReducerClass(Ex2SectorReducer_Companies.class);
		
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
			System.out.println("Usage: Job2_companies .../historical_stock_prices.csv .../historical_stocks.csv .../RISULTATO_JOB2");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new Ex2_companies_combiner(), args);
		System.exit(res);
	}
}
