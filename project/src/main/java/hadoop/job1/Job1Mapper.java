package hadoop.job1;


import java.io.IOException;
import java.time.LocalDate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job1Mapper extends Mapper<LongWritable, Text, Text, Job1TupleWritable> {


	private static final String COMMA = ",";
	//	private static final String DASH = "-";

	private Text ticker = new Text();



	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		try {
            if (key.get() == 0 /*&& value.toString().contains("header")*/) /*Some condition satisfying it is header*/
                return;
            else {
            	String line = value.toString();
        		String[] tokens = line.split(COMMA);

        		//		String date = tokens[7];
        		//		String[] dateTokens = date.split(DASH);
        		//		Integer year = Integer.valueOf(dateTokens[0]);
        		if(tokens.length==8) {
        			LocalDate date = LocalDate.parse(tokens[7]);
        			if(date.getYear()>=2008) {
        				float close = Float.valueOf(tokens[2]);
        				float low = Float.valueOf(tokens[4]);
        				float high = Float.valueOf(tokens[5]);
        				float volume = Float.valueOf(tokens[6]);

        				ticker.set(tokens[0]);
        				Job1TupleWritable j1T = new Job1TupleWritable(date,close,low,high,volume);
        				context.write(ticker, j1T);
        			}
        		}
            }
        } catch (Exception e) {
            return;
        }
		
		
		
	}
}