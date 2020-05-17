package hadoop.ex2_new;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Job2MapperCompany_withCompany extends Mapper<Text,Text,Text,Text>{

	private static final String COMMA = ",";

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		try {			
			String line = value.toString();
			String[] tokens = line.split(COMMA);

			if(tokens.length==6)
				context.write(new Text(key.toString()), new Text(line));
		}
		catch(Exception e) {
			e.printStackTrace();
			return;
		}
	}
}
