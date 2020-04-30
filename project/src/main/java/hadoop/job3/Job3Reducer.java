package hadoop.job3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 
 * 
 * 
 * 
 *
 *
 *
 */
public class Job3Reducer extends Reducer<Text,Text,Text,Text>{

	private static final String COMMA = ",";
	
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

		String outputKey = "{";
		boolean isFirst = true;
		
		for(Text t : values) {
			if(isFirst) {
				outputKey += t;
				isFirst = false;
			}
			else
				outputKey += COMMA + t;
		}
		
		context.write(new Text(outputKey), key);
	}
	
	
}
