package hadoop.job3;

import java.io.IOException;
import java.time.LocalDate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 

 *
 *
 */
public class Job3HSPMapper extends Mapper<LongWritable, Text, Text, Text>{

	
	private static final String COMMA = ",";
	private static final String SEPARATOR_HSP = "hsp";
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			if (key.get() == 0) /*&& value.toString().contains("header")*/ /*Some condition satisfying it is header*/
				return;
			else {
				String line = value.toString();
				String[] tokens = line.split(COMMA);

				LocalDate date = LocalDate.parse(tokens[7]);

				if(date.getYear()>=2016 && date.getYear()<=2018) {
					if(tokens.length==8) {
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