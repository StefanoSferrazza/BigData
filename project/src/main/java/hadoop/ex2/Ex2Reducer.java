package hadoop.ex2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Ex2Reducer extends Reducer<Text,Text,Text,Text>{

	private static final String COMMA = ",";
	private static final String TAB = "\t";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		context.write(new Text("SETTORE" + COMMA + "ANNO"), new Text("VOLUME_ANNUALE_MEDIO" + COMMA + "VARIAZIONE_ANNUALE_MEDIA_%" + COMMA + "QUOTAZIONE_GIORNALIERA_MEDIA"));
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

		try {
			
			long sumVolume = 0;
			float sumLastCloses = 0;
			float sumFirstCloses = 0;
			float sumDailyCloses = 0;
			long yearRows = 0;
			int counterRows = 0;

			for(Text value : values) {
				String line = value.toString();
				String[] tokens = line.split(COMMA);
				if(tokens.length==5) {
					counterRows++;
					sumVolume += Long.parseLong(tokens[0]);
					sumLastCloses += Float.parseFloat(tokens[1]);
					sumFirstCloses += Float.parseFloat(tokens[2]);
					sumDailyCloses += Float.parseFloat(tokens[3]);
					yearRows += Long.parseLong(tokens[4]);
				}
			}

			Long avgSumVolume = (Long)(sumVolume/counterRows);	
			float avgYearVar = ((sumLastCloses-sumFirstCloses)/sumFirstCloses)*100;
			float avgDailyClose = sumDailyCloses/yearRows;
			
			avgYearVar = ((float)Math.round(avgYearVar*100))/100;
			avgDailyClose = ((float)Math.round(avgDailyClose*100))/100;
			
			String[] keys = key.toString().split(COMMA);
			
			//("SETTORE, ANNO"), ("VOLUME_ANNUALE_MEDIO,VARIAZIONE_ANNUALE_MEDIA,QUOTAZIONE_GIORNALIERA_MEDIA"));
			context.write(new Text(keys[0] + COMMA + keys[1]), new Text(avgSumVolume + COMMA + avgYearVar + "%" + COMMA + avgDailyClose));
		}
		catch(Exception e) {
			e.printStackTrace();
			return;
		}
	}
	

}
