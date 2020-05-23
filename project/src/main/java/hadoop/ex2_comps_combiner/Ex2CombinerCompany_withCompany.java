package hadoop.ex2_comps_combiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Ex2CombinerCompany_withCompany extends Reducer<Text,Text,Text,Text>{

	private static final String COMMA = ",";
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		//	<(company,year), (sumVolume,lastClose,firstClose,sumDailyClose,yearRow,sector)>
		try {
			long sumYearVolumeCompany = 0;
			float sumLastCloses = 0;
			float sumFirstCloses = 0;
			float sumDailyCloses = 0;
			long yearRowsEachTicker = 0;
			for(Text value : values) {
				String line = value.toString();
				String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				if(tokens.length==5) {
					sumYearVolumeCompany += Long.parseLong(tokens[0]);
					sumLastCloses += Float.parseFloat(tokens[1]);
					sumFirstCloses += Float.parseFloat(tokens[2]);
					sumDailyCloses += Float.parseFloat(tokens[3]);
					yearRowsEachTicker += Long.parseLong(tokens[4]);
				}
			}
			
			
			context.write(key, new Text(sumYearVolumeCompany + COMMA + sumLastCloses + COMMA + sumFirstCloses + COMMA + sumDailyCloses + COMMA + yearRowsEachTicker));

		}
		catch(Exception e) {
			e.printStackTrace();
			return;
		}
	}

}
