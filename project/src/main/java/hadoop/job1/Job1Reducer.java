package hadoop.job1;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.time.LocalDate;

import org.apache.hadoop.io.Text;

public class Job1Reducer extends Reducer<Text, Text, Text, Text>{

	private static final String COMMA = ",";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		context.write(new Text("SETTORE" + COMMA + "ANNO"), new Text("VOLUME_ANNUALE_MEDIO" + COMMA + "VARIAZIONE_ANNUALE_MEDIA_%" + COMMA + "QUOTAZIONE_GIORNALIERA_MEDIA"));
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//declaration and dummy initialization, will be overwritten when sees first value
		float actualInitialCloseValue = 0;					//to calculate final percentageChange
		float actualFinalCloseValue = 0;					//to calculate final percentageChange
		LocalDate actualMinDate = LocalDate.now();			//to take correct initialCloseValue (first close in time)
		LocalDate actualMaxDate = LocalDate.now();			//to take correct finalCloseValue (last close in time)
		float actualMinPrice = 999999999;		
		float actualMaxPrice = 0;				
		int counterTuples = 0;							//to calculate avg of volumes
		long sumVolumes = 0;								//to calculate avg of volumes
		boolean valuesInitialized = false;					//to initialize on first iteration

		//<ticker,(date,close,volume)>
		for(Text value : values) {
			
			String line = value.toString();
			String[] tokens = line.split(COMMA);
			
			LocalDate date = LocalDate.parse(tokens[0]);
			float close = Float.parseFloat(tokens[1]);
			long volume = Long.parseLong(tokens[2]);
			
			if(!valuesInitialized) {						//initialization
				actualInitialCloseValue = close;
				actualFinalCloseValue = close;
				actualMinDate = date;
				actualMaxDate = date;
				actualMinPrice = close;
				actualMaxPrice = close;
				valuesInitialized = true;
			}
			if(actualMinDate.isAfter(date)) {			//update initialCloseValue if current tuple date is lower
				actualMinDate = date;
				actualInitialCloseValue = close;
			}
			if(actualMaxDate.isBefore(date)) {			//update initialCloseValue if current tuple date is greater
				actualMaxDate = date;
				actualFinalCloseValue = close;
			}
			if(close<actualMinPrice) {					//if current tuple min value is lower, update min value
				actualMinPrice = close;
			}
			if(close>actualMaxPrice) {					//if current tuple max value is greater, update max value
				actualMaxPrice = close;
			}
			counterTuples++;									
			sumVolumes += volume;
		}
		
		float percentageChange = ((actualFinalCloseValue - actualInitialCloseValue) / actualInitialCloseValue)*100;

		float avgVolume = sumVolumes / counterTuples;
		
		Text ticker = key;
		
		context.write(ticker, new Text(percentageChange + COMMA + actualMinPrice + COMMA + actualMaxPrice + COMMA + avgVolume));
	}
}
