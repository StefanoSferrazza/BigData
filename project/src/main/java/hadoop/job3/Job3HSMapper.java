package hadoop.job3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 
 * 
 * 
 * 
 *
 */
public class Job3HSMapper extends Mapper<LongWritable, Text, Text, Text>{

	private static final String COMMA = ",";
	private static final String SEPARATOR_HS = "hs";

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			if (key.get() == 0) /*&& value.toString().contains("header")*/ /*Some condition satisfying it is header*/
				return;
			else {
				String line = value.toString();
				String[] tokens = line.split(COMMA);
				
				if(tokens.length==5) {
					context.write(new Text(tokens[0]), new Text(SEPARATOR_HS + COMMA + tokens[2]));
				}
			}
		}
		catch (Exception e) {
			return;
		}
	}
	

}
