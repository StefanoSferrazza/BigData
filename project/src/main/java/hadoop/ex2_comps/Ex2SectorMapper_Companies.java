package hadoop.ex2_comps;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 
 *
 *
 *
 * 
 *
 */
public class Ex2SectorMapper_Companies extends Mapper<Text,Text,Text,Text>{

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		try {			
//			String keys = key.toString();
//			String[] tokensKeys = keys.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
//			if(tokensKeys[0].equals("")) {
//				throw new Exception();		//controlla che al precedente recuder sia stato inserito correttamente un settore
//			}
//			else {
				String line = value.toString();
				String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

				if(tokens.length==3) //4)
					context.write(new Text(key.toString()), new Text(line));
//			}
		}
		catch(Exception e) {
			e.printStackTrace();
			return;
		}
	}
}
