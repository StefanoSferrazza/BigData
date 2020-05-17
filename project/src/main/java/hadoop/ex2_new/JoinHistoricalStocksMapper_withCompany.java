package hadoop.ex2_new;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import utilities.Utilities;

public class JoinHistoricalStocksMapper_withCompany extends Mapper<LongWritable, Text, Text, Text>{

	private static final String COMMA = ",";
	private static final String SEPARATOR_HS = "historical_stock";

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		try {
			if (key.get() == 0)
				return;
			else {
				String line = value.toString();
				String[] tokens = line.split(COMMA);
				
							
				if(tokens.length==5	&&
	        			Utilities.inputExists(tokens[0]) &&	//ticker
	        			Utilities.inputExists(tokens[2]) &&	//company
	        			Utilities.inputExists(tokens[3])) {	//sector
					
					//<ticker, (historical_stock,company,sector)>						
					context.write(new Text(tokens[0]), new Text(SEPARATOR_HS + COMMA + tokens[2] + COMMA + tokens[3]));
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
}
