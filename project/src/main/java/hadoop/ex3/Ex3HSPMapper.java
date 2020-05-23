package hadoop.ex3;

import java.io.IOException;
import java.time.LocalDate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utilities.Utilities;

/**
 * 
 * Mapper for Historical_Stock_Prices
 * 
 */
public class Ex3HSPMapper extends Mapper<LongWritable, Text, Text, Text>{


	private static final String COMMA = ",";
	private static final String SEPARATOR_HSP = "hsp";		// for the join

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			if (key.get() == 0) /*Some condition satisfying it is header*/
				return;
			else {
				String line = value.toString();
				String[] tokens = line.split(COMMA);

				/*check input correctness*/
				if(tokens.length==8) {
					LocalDate date = LocalDate.parse(tokens[7]);

					if(date.getYear()>=2016 && date.getYear()<=2018  &&
	        				Utilities.inputExists(tokens[0]) &&
	        				Utilities.inputExists(tokens[2]) &&
	        				Utilities.inputExists(tokens[7])) {
						
						Float.parseFloat(tokens[2]);

						//	<ticker, (historical_stock_prices,close,date)>
						context.write(new Text(tokens[0]), new Text(SEPARATOR_HSP + COMMA + tokens[2] + COMMA + tokens[7]));
					}
				}
			}
		}
		catch (Exception e) {
			return;
		}
	}


}
