package hadoop.job3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * 
 * 
 * 
 * 
 *
 */
public class Job3 extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		
		Path temp = new Path("temp");
		Path inputHSP = new Path(args[0]);
		Path inputHS = new Path(args[1]);
		Path output = new Path(args[2]);

		Configuration conf = getConf();

		@SuppressWarnings("deprecation")
		Job join = new Job(conf, "join");
		join.setJarByClass(Job3.class);

		MultipleInputs.addInputPath(join, inputHSP,TextInputFormat.class, Job3HSPMapper.class);
		MultipleInputs.addInputPath(join, inputHS,TextInputFormat.class, Job3HSMapper.class);
		FileOutputFormat.setOutputPath(join, temp);

		join.setReducerClass(Job3JoinReducer.class);
		join.setOutputKeyClass(Integer.class);
		join.setOutputValueClass(Text.class);
		join.setOutputFormatClass(TextOutputFormat.class);

		boolean succ = join.waitForCompletion(true);

		if (! succ) {
			System.out.println("Join failed, exiting");
			return -1;
		}

		@SuppressWarnings("deprecation")
		Job job3 = new Job(conf, "job3");
		join.setJarByClass(Job3.class);

		FileInputFormat.setInputPaths(job3, temp);
		FileOutputFormat.setOutputPath(job3, output);

		job3.setMapperClass(Job3Mapper.class);
		job3.setReducerClass(Job3Reducer.class);

		succ = job3.waitForCompletion(true);

		if (! succ) {
			System.out.println("Job3 failed, exiting");
			return -1;
		}

		return 0;
	}


	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: TopKRecords /path/to/citation.txt output_dir");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new Job3(), args);
		System.exit(res);
	}
	

}
