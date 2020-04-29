package hadoop.job2;

import java.io.IOException;
import java.time.LocalDate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2JoinReducer extends Reducer<Text, Text, Text, Text>{
	
	private static final String COMMA = ",";
	private static final String SEPARATOR_HS = "historical_stock";
	private static final String SEPARATOR_HSP = "historical_stock_prices";

	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		for(Text value : values) {
			
			String line = value.toString();
			String[] tokens = line.split(COMMA);
			
			String sector = "";
			String close = "";
			String volume = "";
			String date = "";
			if(tokens[0].equals(SEPARATOR_HS)) {
				sector = tokens[1];
			}
			
			else if(tokens[0].equals(SEPARATOR_HSP)) {
				close = tokens[1];
				volume = tokens[2];
				date = tokens[3];
			}
			//	<ticker, (sector,close,volume,date)>
			context.write(new Text(key), new Text(sector + COMMA + close + COMMA + volume + COMMA + date));
		}
	}

}
