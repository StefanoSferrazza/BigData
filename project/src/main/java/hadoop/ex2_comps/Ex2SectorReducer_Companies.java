package hadoop.ex2_comps;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 
 * Sector Reducer for Job2
 * 
 */
public class Ex2SectorReducer_Companies extends Reducer<Text,Text,Text,Text>{

	private static final String COMMA = ",";


	/**
	 * Set first record as the header with the names of the columns 
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		context.write(new Text("SETTORE" + COMMA + "ANNO" + COMMA), new Text("VOLUME_ANNUALE_MEDIO" + COMMA + "VARIAZIONE_ANNUALE_MEDIA_%" + COMMA + "QUOTAZIONE_GIORNALIERA_MEDIA"));
	}



	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		try {
			long sectorSumVolume = 0;
			float sectorSumDeltaQuotation = 0;
			float sectorSumDailyClose = 0;
			int counterCompanies = 0;

			for(Text value : values) {
				String line = value.toString();
				String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				if(tokens.length==4) {
					sectorSumVolume += Long.parseLong(tokens[0]);
					sectorSumDeltaQuotation += Float.parseFloat(tokens[1]);
					sectorSumDailyClose += Float.parseFloat(tokens[2]);
					counterCompanies +=Integer.parseInt(tokens[3]);
				}
			}

			/*calculate avgSumVolume based on its definition*/
			Long avgSumVolume = (Long)(sectorSumVolume / counterCompanies);
			/*calculate avgDeltaQuot based on its definition*/
			float avgDeltaQuot = sectorSumDeltaQuotation / counterCompanies;
			/*calculate avgDailyClose based on its definition*/
			float avgDailyClose = sectorSumDailyClose / counterCompanies;
			
			/*round them*/
			avgDeltaQuot = ((float)Math.round(avgDeltaQuot*100))/100;
			avgDailyClose = ((float)Math.round(avgDailyClose*100))/100;
			
			
			//("SETTORE, ANNO"), ("VOLUME_ANNUALE_MEDIO,VARIAZIONE_ANNUALE_MEDIA,QUOTAZIONE_GIORNALIERA_MEDIA"));
			context.write(new Text(key.toString() + COMMA), new Text(avgSumVolume + COMMA + avgDeltaQuot + "%" + COMMA + avgDailyClose));
		}
		catch(Exception e) {
			e.printStackTrace();
			return;
		}
	}
}
