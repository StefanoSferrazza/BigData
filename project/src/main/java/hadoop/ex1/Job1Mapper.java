package hadoop.ex1;

import java.io.IOException;
import java.time.LocalDate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utilities.Utilities;


public class Job1Mapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final String COMMA = ",";

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		try {
			if (key.get() == 0)
				return;

			String[] tokens = value.toString().split(COMMA);

			if(tokens.length==8 &&
					Utilities.inputExists(tokens[0]) &&		//ticker
					Utilities.inputExists(tokens[2]) &&		//close
					Utilities.inputExists(tokens[6]) &&		//volume
					Utilities.inputExists(tokens[7])) {		//date

				LocalDate date = LocalDate.parse(tokens[7]);

				if(date.getYear()>=2008 && date.getYear()<=2018) {
					Text ticker = new Text(tokens[0]);
					String close = tokens[2];
					String volume = tokens[6];
					String dateString = date.toString();

					context.write(ticker, new Text(dateString + COMMA + close + COMMA + volume));
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			return;
		}
	}
	
	
}