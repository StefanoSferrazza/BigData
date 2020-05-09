package hadoop.job1;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

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
	private List<Result> results;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		context.write(new Text("TICKER"), new Text("VARIAZIONE_QUOTAZIONE_%" + COMMA + "PREZZO_MIN" + COMMA + "PREZZO_MAX" + COMMA + "VOLUME_MEDIO"));
		results = new LinkedList<Result>();
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

		results.add(new Result(ticker,percentageChange,minPrice,maxPrice,avgVolume));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		Collections.sort(results);
		for(Result r : results) {
			context.write(	new Text(	r.getTicker()), 
					new Text(	r.getPercentageChange() + COMMA + 
							r.getMinPrice() + COMMA + 
							r.getMaxPrice() + COMMA + 
							r.getAvgVolume()));
		}
	}


	private class Result implements Comparable<Result>{

		private String ticker;
		private Float percentageChange;
		private Float minPrice;
		private Float maxPrice;
		private Long avgVolume;
		
		public Result(String ticker, Float percentageChange, Float minPrice, Float maxPrice, Long avgVolume) {
			this.ticker = ticker;
			this.percentageChange = percentageChange;
			this.minPrice = minPrice;
			this.maxPrice = maxPrice;
			this.avgVolume = avgVolume;
		}

		public int compareTo(Result r) {
			return r.getPercentageChange().compareTo(this.percentageChange);		//ordine decrescente
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getEnclosingInstance().hashCode();
			result = prime * result + ((avgVolume == null) ? 0 : avgVolume.hashCode());
			result = prime * result + ((maxPrice == null) ? 0 : maxPrice.hashCode());
			result = prime * result + ((minPrice == null) ? 0 : minPrice.hashCode());
			result = prime * result + ((percentageChange == null) ? 0 : percentageChange.hashCode());
			result = prime * result + ((ticker == null) ? 0 : ticker.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Result other = (Result) obj;
			if (!getEnclosingInstance().equals(other.getEnclosingInstance()))
				return false;
			if (avgVolume == null) {
				if (other.avgVolume != null)
					return false;
			} else if (!avgVolume.equals(other.avgVolume))
				return false;
			if (maxPrice == null) {
				if (other.maxPrice != null)
					return false;
			} else if (!maxPrice.equals(other.maxPrice))
				return false;
			if (minPrice == null) {
				if (other.minPrice != null)
					return false;
			} else if (!minPrice.equals(other.minPrice))
				return false;
			if (percentageChange == null) {
				if (other.percentageChange != null)
					return false;
			} else if (!percentageChange.equals(other.percentageChange))
				return false;
			if (ticker == null) {
				if (other.ticker != null)
					return false;
			} else if (!ticker.equals(other.ticker))
				return false;
			return true;
		}
		
		public String getTicker() { return ticker; }
		public void setTicker(String ticker) { this.ticker = ticker; }
		public Float getPercentageChange() { return percentageChange; }
		public void setPercentageChange(Float percentageChange) { this.percentageChange = percentageChange; }
		public Float getMinPrice() { return minPrice; }
		public void setMinPrice(Float minPrice) { this.minPrice = minPrice; }
		public Float getMaxPrice() { return maxPrice; }
		public void setMaxPrice(Float maxPrice) { this.maxPrice = maxPrice; }
		public Long getAvgVolume() { return avgVolume; }
		public void setAvgVolume(Long avgVolume) { this.avgVolume = avgVolume; }


		private Job1Reducer getEnclosingInstance() {
			return Job1Reducer.this;
		}

		
	}
	
}
