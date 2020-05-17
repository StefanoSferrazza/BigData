package hadoop.ex2_new;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Job2ReducerSector_withCompany extends Reducer<Text,Text,Text,Text>{

	private static final String COMMA = ",";
	private static final String TAB = "\t";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		context.write(new Text("SETTORE" + COMMA + "ANNO"), new Text("VOLUME_ANNUALE_MEDIO" + COMMA + "VARIAZIONE_ANNUALE_MEDIA_%" + COMMA + "QUOTAZIONE_GIORNALIERA_MEDIA"));
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
//		<(sector,year), (sumYearVolumeCompany,yearVarCompany,avgDailyCloseCompany)>
		try {
			
			long sumVolume = 0;
			float sumVar = 0;
			float sumAvgDailyCloses = 0;
			int counterCompanies = 0;

			for(Text value : values) {
				String line = value.toString();
				String[] tokens = line.split(COMMA);
				if(tokens.length==3) {
					sumVolume+=Long.parseLong(tokens[0]);
					sumVar+=Float.parseFloat(tokens[1]);
					sumAvgDailyCloses+=Float.parseFloat(tokens[2]);
					counterCompanies++;
				}
			}

			long avgVolume = Math.round((float)(sumVolume / counterCompanies));
			float avgVar = Math.round(sumVar / counterCompanies);
			float avgAvgDailyCloses = Math.round(sumAvgDailyCloses / counterCompanies);
			
			
			//("SETTORE, ANNO"), ("VOLUME_ANNUALE_MEDIO,VARIAZIONE_ANNUALE_MEDIA,QUOTAZIONE_GIORNALIERA_MEDIA"));
			context.write(new Text(key.toString()), new Text(avgVolume + COMMA + avgVar + "%" + COMMA + avgAvgDailyCloses));
		}
		catch(Exception e) {
			e.printStackTrace();
			return;
		}
	}
}
