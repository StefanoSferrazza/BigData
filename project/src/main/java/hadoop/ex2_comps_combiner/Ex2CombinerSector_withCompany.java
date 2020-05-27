package hadoop.ex2_comps_combiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner 2 for Job2 Complex Version 
 */
public class Ex2CombinerSector_withCompany extends Reducer<Text,Text,Text,Text>{

	private static final String COMMA = ",";

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
//		<(sector,year), (sumYearVolumeCompany,yearVarCompany,avgDailyCloseCompany)>
		try {
			/*initialize*/
			long sumVolume = 0;
			float sumVar = 0;
			float sumAvgDailyCloses = 0;
			int counterCompanies = 0;

			/*update*/
			for(Text value : values) {
				String line = value.toString();
				String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				if(tokens.length==4) {
					sumVolume+=Long.parseLong(tokens[0]);
					sumVar+=Float.parseFloat(tokens[1]);
					sumAvgDailyCloses+=Float.parseFloat(tokens[2]);
					counterCompanies+=Integer.parseInt(tokens[3]);
				}
			}
			context.write(key, new Text(sumVolume + COMMA + sumVar + COMMA + sumAvgDailyCloses + COMMA + counterCompanies));
		}
		catch(Exception e) {
			e.printStackTrace();
			return;
		}
	}
}
