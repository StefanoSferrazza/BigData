package hadoop.job3;

import java.io.IOException;

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
public class Job3Mapper extends Mapper<Text, Text, Text, Text>{

	private static final String COMMA = ",";


	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			String[] tokens = line.split(COMMA);

			if(tokens.length == 3)
				context.write(new Text(line), new Text(key));

		}
		catch (Exception e) {
			return;
		}
	}


}
