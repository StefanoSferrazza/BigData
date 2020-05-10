package hadoop.job1;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import utilities.Result_Job1;
import utilities.Utilities;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;

public class Job1Reducer extends Reducer<Text, Text, Text, Text>{

	private static final String COMMA = ",";
	private List<Result_Job1> results;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		context.write(new Text("TICKER"), new Text("VARIAZIONE_QUOTAZIONE_%" + COMMA + "PREZZO_MIN" + COMMA + "PREZZO_MAX" + COMMA + "VOLUME_MEDIO"));
		results = new LinkedList<Result_Job1>();
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//declaration and dummy initialization, will be overwritten when sees first value
		float actualInitialCloseValue = 0;					//to calculate final percentageChange
		float actualFinalCloseValue = 0;					//to calculate final percentageChange
		LocalDate actualMinDate = LocalDate.now();			//to take correct initialCloseValue (first close in time)
		LocalDate actualMaxDate = LocalDate.now();			//to take correct finalCloseValue (last close in time)
		float minPrice = 999999999;		
		float maxPrice = 0;				
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
				minPrice = close;
				maxPrice = close;
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
			if(close<minPrice) {					//if current tuple min value is lower, update min value
				minPrice = close;
			}
			if(close>maxPrice) {					//if current tuple max value is greater, update max value
				maxPrice = close;
			}
			counterTuples++;									
			sumVolumes += volume;
		}

		float percentageChange = ((actualFinalCloseValue - actualInitialCloseValue) / actualInitialCloseValue)*100;

		long avgVolume = sumVolumes/counterTuples;

		percentageChange = Utilities.truncateToSecondDecimal(percentageChange);
		//		minPrice = u.truncateToSecondDecimal(minPrice);
		//		maxPrice = u.truncateToSecondDecimal(maxPrice);

		String ticker = key.toString();

		results.add(new Result_Job1(ticker,percentageChange,minPrice,maxPrice,avgVolume));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		Collections.sort(results);
		for(Result_Job1 r : results) {
			context.write(	new Text(	r.getTicker()), 
					new Text(r.toString()));
		}
	}

	
	
}
