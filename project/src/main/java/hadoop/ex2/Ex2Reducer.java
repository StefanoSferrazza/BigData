package hadoop.ex2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 
 * 
 * 
 *
 */
public class Ex2Reducer extends Reducer<Text,Text,Text,Text>{

	private static final String COMMA = ",";
	
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
			long sectorYearRows = 0;
			int counterRows = 0;

			for(Text value : values) {
				String line = value.toString();
				String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				if(tokens.length==4) {
					sectorSumVolume += Long.parseLong(tokens[0]);
					sectorSumDeltaQuotation += Float.parseFloat(tokens[1]);
					sectorSumDailyClose += Float.parseFloat(tokens[2]);
					sectorYearRows += Long.parseLong(tokens[3]);
					counterRows++;
				}
			}

			Long avgSumVolume = (Long)(sectorSumVolume/counterRows);	
			float avgDeltaQuot = sectorSumDeltaQuotation/counterRows;
			float avgDailyClose = sectorSumDailyClose/sectorYearRows;
			
			avgDeltaQuot = ((float)Math.round(avgDeltaQuot*100))/100;
			avgDailyClose = ((float)Math.round(avgDailyClose*100))/100;
			
			String[] keys = key.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			
			//("SETTORE, ANNO"), ("VOLUME_ANNUALE_MEDIO,VARIAZIONE_ANNUALE_MEDIA,QUOTAZIONE_GIORNALIERA_MEDIA"));
			context.write(new Text(keys[0] + COMMA + keys[1]), new Text(avgSumVolume + COMMA + avgDeltaQuot + "%" + COMMA + avgDailyClose));
		}
		catch(Exception e) {
			e.printStackTrace();
			return;
		}
	}
	

}
