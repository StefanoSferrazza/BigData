package hadoop.ex1;

import org.apache.hadoop.mapreduce.Reducer;

import utilities.Utilities;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;


/**
 * 
 * 
 * 
 * 
 *
 */
public class Ex1Reducer extends Reducer<Text, Text, Text, Text>{

	private static final String COMMA = ",";
	private List<Result_Ex1> results;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		context.write(new Text("TICKER" + COMMA), new Text("VARIAZIONE_QUOTAZIONE_%" + COMMA + "PREZZO_MIN" + COMMA + "PREZZO_MAX" + COMMA + "VOLUME_MEDIO"));
		results = new LinkedList<Result_Ex1>();
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//declaration and dummy initialization, will be overwritten when sees first value
		float firstClose = 0;					//to calculate final percentageChange
		float lastClose = 0;					//to calculate final percentageChange
		LocalDate firstDate = LocalDate.now();			//to take correct initialCloseValue (first close in time)
		LocalDate lastDate = LocalDate.now();			//to take correct finalCloseValue (last close in time)
		float minClose = 999999999;		
		float maxClose = 0;				
		int counterTuples = 0;								//to calculate avg of volumes
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

		int percentageChange = Math.round(((lastClose - firstClose) / firstClose)*100);

		long avgVolume = sumVolumes/counterTuples;

		String ticker = key.toString();

		results.add(new Result_Ex1(ticker,percentageChange,minClose,maxClose,avgVolume));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		Collections.sort(results);
		for(Result_Ex1 r : results) {
			context.write(	new Text(	r.getTicker()), 
					new Text(r.toString()));
		}
	}

	
	private class Result_Ex1 implements Comparable<Result_Ex1>,Serializable{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		private static final String COMMA = ",";
		
		private String ticker;
		private Integer percentageChange;
		private Float minPrice;
		private Float maxPrice;
		private Long avgVolume;
		
		public Result_Ex1(String ticker, Integer percentageChange, Float minPrice, Float maxPrice, Long avgVolume) {
			this.ticker = ticker;
			this.percentageChange = percentageChange;
			this.minPrice = minPrice;
			this.maxPrice = maxPrice;
			this.avgVolume = avgVolume;
		}

		public int compareTo(Result_Ex1 r) {
			return r.getPercentageChange().compareTo(this.percentageChange);		//ordine decrescente
		}
		
		@Override
		public String toString() {
			return this.getPercentageChange() + "%" + COMMA + 
					this.getMinPrice() + COMMA + 
					this.getMaxPrice() + COMMA + 
					this.getAvgVolume();
		}
		
		

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
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
			Result_Ex1 other = (Result_Ex1) obj;
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
		public Integer getPercentageChange() { return percentageChange; }
		public void setPercentageChange(Integer percentageChange) { this.percentageChange = percentageChange; }
		public Float getMinPrice() { return minPrice; }
		public void setMinPrice(Float minPrice) { this.minPrice = minPrice; }
		public Float getMaxPrice() { return maxPrice; }
		public void setMaxPrice(Float maxPrice) { this.maxPrice = maxPrice; }
		public Long getAvgVolume() { return avgVolume; }
		public void setAvgVolume(Long avgVolume) { this.avgVolume = avgVolume; }

	}
}
