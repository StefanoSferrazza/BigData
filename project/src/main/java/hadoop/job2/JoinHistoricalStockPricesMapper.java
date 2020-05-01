package hadoop.job2;

import java.io.IOException;
import java.time.LocalDate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class JoinHistoricalStockPricesMapper extends Mapper<LongWritable, Text, Text, Text>{

	private static final String COMMA = ",";
	private static final String SEPARATOR_HSP = "historical_stock_prices";

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		try {
			if (key.get() == 0) /*Some condition satisfying it is header*/
				return;
			else {
				String line = value.toString();
				String[] tokens = line.split(COMMA);

				LocalDate date = LocalDate.parse(tokens[7]);

				
				if(date.getYear()>=2008 && date.getYear()<=2018) {
					if(tokens.length==8 &&
							!(tokens[0].equals(null) || tokens[0].equals("") || tokens[0].equals("N/A")) &&
							!(tokens[2].equals(null) || tokens[2].equals("") || tokens[2].equals("N/A")) &&
							!(tokens[6].equals(null) || tokens[6].equals("") || tokens[6].equals("N/A")) &&
							!(tokens[7].equals(null) || tokens[7].equals("") || tokens[7].equals("N/A")) ) {
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