package hadoop.job2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class JoinHistoricalStocksMapper extends Mapper<LongWritable, Text, Text, Text>{

		private static final String COMMA = ",";
		private static final String SEPARATOR_HS = "historical_stock";

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			try {
				if (key.get() == 0)
					return;
				else {
					String line = value.toString();
					String[] tokens = line.split(COMMA);
					
								
					if(tokens.length==5 &&
							!(tokens[0].equals(null) || tokens[0].equals("") || tokens[0].equals("N/A")) &&
							!(tokens[3].equals(null) || tokens[3].equals("") || tokens[3].equals("N/A")) ) {
						//<ticker, (historical_stock,sector)>
						
						context.write(new Text(tokens[0]), new Text(SEPARATOR_HS + COMMA + tokens[3]));
					}
				}
			}
			catch (Exception e) {
				e.printStackTrace();
				return;
			}
		}

}
