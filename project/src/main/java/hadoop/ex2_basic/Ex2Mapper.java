package hadoop.ex2_basic;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 
 * Mapper for Job2
 * 
 */
public class Ex2Mapper extends Mapper<Text,Text,Text,Text>{

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		try {			
			String line = value.toString();
			String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

			/*check input correctness*/
			if(tokens.length==4)
				//	<(sector,year), (sumVolume,deltaQuotation,sumDailyClose,yearRow)>
				context.write(new Text(key.toString()), new Text(line));
		}
		catch(Exception e) {
			e.printStackTrace();
			return;
		}
	}
}
