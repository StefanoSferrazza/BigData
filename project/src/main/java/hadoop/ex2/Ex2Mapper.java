package hadoop.ex2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 
 * 
 * 
 * 
 *
 */
public class Ex2Mapper extends Mapper<Text,Text,Text,Text>{

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		try {			
			String line = value.toString();
			String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

			if(tokens.length==4)
				context.write(new Text(key.toString()), new Text(line));
		}
		catch(Exception e) {
			e.printStackTrace();
			return;
		}
	}
}
