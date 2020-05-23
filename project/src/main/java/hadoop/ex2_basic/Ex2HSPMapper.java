package hadoop.ex2_basic;

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
public class Ex2HSPMapper extends Mapper<LongWritable, Text, Text, Text>{

	private static final String COMMA = ",";
	private static final String SEPARATOR_HSP = "historical_stock_prices";		// for the join

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

					if(date.getYear()>=2008 && date.getYear()<=2018  &&
							Utilities.inputExists(tokens[0]) &&
							Utilities.inputExists(tokens[2]) &&
							Utilities.inputExists(tokens[6]) &&
							Utilities.inputExists(tokens[7]) ) {

						Float.parseFloat(tokens[2]);
						Long.parseLong(tokens[6]);

						//	<ticker, (historical_stock_prices,close,volume,date)>
						context.write(new Text(tokens[0]), new Text(SEPARATOR_HSP + COMMA + tokens[2] + COMMA + tokens[6] + COMMA + tokens[7]));					
					}
				}
			}
		}
		catch (Exception e) {
			return;
		}
	}

}