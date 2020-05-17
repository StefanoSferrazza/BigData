package hadoop.ex3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



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
	protected void setup(Context context) throws IOException, InterruptedException{
		
		context.write(new Text( " { AZIENDE_CON_TREND_COMUNE }:") , new Text("2016: VAR_ANN_%" + COMMA + "2017: VAR_ANN_%" + COMMA + "2018: VAR_ANN_%"));
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

		String outputKey = "{";
		boolean isFirst = true;
		int counter = 0;

		for(Text t : values) {
			if(isFirst) {
				outputKey += t;
				isFirst = false;
			}
			else
				outputKey += COMMA + t;
			counter++;
		}
		outputKey += "}:";

		if(counter > 1)
			context.write(new Text(outputKey), key);
	}


}
