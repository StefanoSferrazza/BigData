package hadoop.ex3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



/**
 * 
 * Mapper for Job3
 * 
 */
public class Ex3Mapper extends Mapper<Text, Text, Text, Text>{


	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String line = value.toString();
			String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

			/*check input correctness*/
			if(tokens.length == 3)
				context.write(new Text(line), new Text(key));
		}
		catch (Exception e) {
			return;
		}
	}


}
