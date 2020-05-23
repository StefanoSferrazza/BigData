package hadoop.ex1;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import utilities.Result_Ex1;

import org.apache.hadoop.io.Text;


/**
 * 
 * Reducer for Job1
 * 
 */
public class Ex1Reducer extends Reducer<Text, Text, Text, Text>{

	private static final String COMMA = ",";
	
	private List<Result_Ex1> results;		//list with tmp results before sorting

	
	/**
	 * Set first record as the header with the names of the columns 
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		context.write(new Text("TICKER" + COMMA), new Text("VARIAZIONE_QUOTAZIONE_%" + COMMA + "PREZZO_MIN" + COMMA + "PREZZO_MAX" + COMMA + "VOLUME_MEDIO"));
		results = new LinkedList<Result_Ex1>();
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		/*declaration and dummy initialization, will be overwritten when sees first value*/
		float firstClose = 0;					//to calculate final deltaQuotation
		float lastClose = 0;					//to calculate final deltaQuotation
		LocalDate firstDate = LocalDate.now();	//to take correct firstClose (first close in time)
		LocalDate lastDate = LocalDate.now();	//to take correct lasttClose (last close in time)
		float minClose = 999999999;		
		float maxClose = 0;				
		int counterTuples = 0;					//to calculate avg of volumes
		long sumVolumes = 0;					//to calculate avg of volumes
		boolean valuesInitialized = false;		//to initialize on first iteration
 
		//<ticker,(date,close,volume)>
		for(Text value : values) {
			
			String line = value.toString();
			String[] tokens = line.split(COMMA);
			
			LocalDate date = LocalDate.parse(tokens[0]);
			float close = Float.parseFloat(tokens[1]);
			long volume = Long.parseLong(tokens[2]);
			
			if(!valuesInitialized) {						//initialization
				firstClose = close;
				lastClose = close;
				firstDate = date;
				lastDate = date;
				minClose = close;
				maxClose = close;
				valuesInitialized = true;
			}
			if(firstDate.isAfter(date)) {			//update initialCloseValue if current tuple date is lower
				firstDate = date;
				firstClose = close;
			}
			if(lastDate.isBefore(date)) {			//update initialCloseValue if current tuple date is greater
				lastDate = date;
				lastClose = close;
			}
			if(close<minClose) {					//if current tuple min value is lower, update min value
				minClose = close;
			}
			if(close>maxClose) {					//if current tuple max value is greater, update max value
				maxClose = close;
			}
			counterTuples++;									
			sumVolumes += volume;
		}

		/*calculate deltaQuotation based on its definition and round it*/
		int deltaQuotation = Math.round(((lastClose - firstClose) / firstClose)*100);

		/*calculate avgVolume based on its definition*/
		long avgVolume = sumVolumes/counterTuples;

		String ticker = key.toString();

		/*build tmp result before sorting*/
		results.add(new Result_Ex1(ticker,deltaQuotation,minClose,maxClose,avgVolume));
	}



	/**
	 * Use the cleanup method to sort the tmp results and produce output 
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		Collections.sort(results);
		for(Result_Ex1 r : results) {
			context.write(new Text(r.getTicker()),new Text(r.toString()));
		}
	}
}
