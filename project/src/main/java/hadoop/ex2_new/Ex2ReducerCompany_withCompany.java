package hadoop.ex2_new;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Ex2ReducerCompany_withCompany extends Reducer<Text,Text,Text,Text>{

	private static final String COMMA = ",";
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		//	<(company,year), (sumVolume,lastClose,firstClose,sumDailyClose,yearRow,sector)>
		try {
			
			long sumYearVolumeCompany = 0;
			float sumLastCloses = 0;
			float sumFirstCloses = 0;
			float sumDailyCloses = 0;
			long yearRowsEachTicker = 0;
			String sector = "";
			for(Text value : values) {
				String line = value.toString();
				String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				if(tokens.length==6) {
					sumYearVolumeCompany += Long.parseLong(tokens[0]);
					sumLastCloses += Float.parseFloat(tokens[1]);
					sumFirstCloses += Float.parseFloat(tokens[2]);
					sumDailyCloses += Float.parseFloat(tokens[3]);
					yearRowsEachTicker += Long.parseLong(tokens[4]);
					sector=tokens[5];
				}
			}

			float yearVarCompany = ((sumLastCloses-sumFirstCloses)/sumFirstCloses)*100;
			float avgDailyCloseCompany = sumDailyCloses/yearRowsEachTicker;
			
			String[] keys = key.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			
//			<(sector,year), (sumYearVolumeCompany,yearVarCompany,avgDailyCloseCompany,counterCompanies)>
			context.write(new Text(sector + COMMA + keys[1]), new Text(sumYearVolumeCompany + COMMA + yearVarCompany + COMMA + avgDailyCloseCompany + COMMA + 1));
		}
		catch(Exception e) {
			e.printStackTrace();
			return;
		}
	}
}
