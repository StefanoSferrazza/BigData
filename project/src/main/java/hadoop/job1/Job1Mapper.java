package hadoop.job1;


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
            else {
            	String line = value.toString();
        		String[] tokens = line.split(COMMA);

        		if(tokens.length==8 &&
        				Utilities.inputExists(tokens[0]) &&
        				Utilities.inputExists(tokens[2]) &&
        				Utilities.inputExists(tokens[6]) &&
        				Utilities.inputExists(tokens[7])) {
        			
        			LocalDate date = LocalDate.parse(tokens[7]);
        			
        			if(date.getYear()>=2008 && date.getYear()<=2018 ) {
        				
        				Text ticker = new Text(tokens[0]);
        				String dateString = tokens[7];
        				String close = tokens[2];
        				String volume = tokens[6];
        				
        				context.write(ticker, new Text(dateString + COMMA + close + COMMA + volume));
        			}
        		}
            }
        } catch (Exception e) {
            return;
        }
		
		
		
	}
}