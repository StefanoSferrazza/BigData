package hadoop.ex2_new;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Job2MapperSector_withCompany extends Mapper<Text,Text,Text,Text>{

	private static final String COMMA = ",";

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		try {			

			String keys = key.toString();
			String[] tokensKeys = keys.split(COMMA);
			if(tokensKeys[0].equals("")) {
				throw new Exception();				//controlla che al precedente recuder sia stato inserito correttamente un settore
			}
			else {
				String line = value.toString();
				String[] tokens = line.split(COMMA);

				if(tokens.length==3)
					context.write(new Text(keys), new Text(line));
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			return;
		}
	}
}
