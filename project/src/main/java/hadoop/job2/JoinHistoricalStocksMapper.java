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
				if (key.get() == 0 /*&& value.toString().contains("header")*/) /*Some condition satisfying it is header*/
					return;
				else {
					String line = value.toString();
					String[] tokens = line.split(COMMA);
					
					if(tokens.length==5) {
						//<ticker, (historical_stock,sector)>
						context.write(new Text(tokens[0]), new Text(SEPARATOR_HS + COMMA + tokens[3]));
					}
				}
			}
			catch (Exception e) {
				return;
			}
		}

}
