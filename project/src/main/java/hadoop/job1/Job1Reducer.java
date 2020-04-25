package hadoop.job1;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Text;

public class Job1Reducer extends Reducer<Text, Job1Tuple, Text, Job1Result>{

	public void Reduce(Text key, Iterable<Job1Tuple> values, Context context) throws IOException, InterruptedException {
		//declaration and dummy initialization, will be overwritten when sees first value
		float actualInitialCloseValue = 0;					//to calculate final percentageChange
		float actualFinalCloseValue = 0;					//to calculate final percentageChange
		Date actualMinDate = new Date();					//to take correct initialCloseValue (first close in time)
		Date actualMaxDate = new Date();					//to take correct finalCloseValue (last close in time)
		float actualMinPrice = 999999999;		
		float actualMaxPrice = 0;				
		float counterTuples = 0;							//to calculate avg of volumes
		float sumVolumes = 0;								//to calculate avg of volumes
		boolean valuesInitialized = false;					//to initialize on first iteration


		for(Job1Tuple t : values) {
			if(!valuesInitialized) {						//initialization
				actualInitialCloseValue = t.getClose();
				actualFinalCloseValue = t.getClose();
				actualMinDate = t.getDate();
				actualMaxDate = t.getDate();
				actualMinPrice = t.getMin();
				actualMaxPrice = t.getMax();
				valuesInitialized = true;
			}
			if(actualMinDate.after(t.getDate())) {			//update initialCloseValue if current tuple date is lower
				actualMinDate = t.getDate();
				actualInitialCloseValue = t.getClose();
			}
			if(actualMaxDate.before(t.getDate())) {			//update initialCloseValue if current tuple date is greater
				actualMaxDate = t.getDate();
				actualFinalCloseValue = t.getClose();
			}
			if(t.getMin()<actualMinPrice) {					//if current tuple min value is lower, update min value
				actualMinPrice = t.getMin();
			}
			if(t.getMax()<actualMaxPrice) {					//if current tuple max value is greater, update max value
				actualMaxPrice = t.getMax();
			}
			counterTuples++;									
			sumVolumes += t.getVolume();
		}
		
		float percentageChange = ((actualFinalCloseValue - actualInitialCloseValue) / actualInitialCloseValue)*100;

		float avgVolume = sumVolumes / counterTuples;
		
		Job1Result result = new Job1Result(percentageChange, actualMinPrice, actualMaxPrice, avgVolume);
		
		context.write(key, result);
	}
}
