package hadoop.job3;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




/**
 * 
 * 
 * 
 * 
 * 
 *
 */
public class Job3Mapper extends Mapper<Text, FloatWritable, Text, Text>{

	private static final String COMMA = ",";

	

	@Override
	protected void map(Text key, FloatWritable value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			String[] tokens = line.split(COMMA);
			
			
			
		}
		catch (Exception e) {
			return;
		}
	}
}
