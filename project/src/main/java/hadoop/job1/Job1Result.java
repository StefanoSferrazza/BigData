package hadoop.job1;

public class Job1Result {

//	private String symbol;
	
	private float percentageChange;
	
	private float minPrice;
	
	private float maxPrice;
	
	private float avgVolume;

	public Job1Result(	
//						String symbol, 
						float percentageChange, 
						float minPrice, 
						float maxPrice, 
						float avgVolume) {
		super();
//		this.symbol = symbol;
		this.percentageChange = percentageChange;
		this.minPrice = minPrice;
		this.maxPrice = maxPrice;
		this.avgVolume = avgVolume;
	}

//	public String getSymbol() {
//		return symbol;
//	}
//
//	public void setSymbol(String symbol) {
//		this.symbol = symbol;
//	}

	public float getPercentageChange() {
		return percentageChange;
	}

	public void setPercentageChange(float percentageChange) {
		this.percentageChange = percentageChange;
	}

	public float getMinPrice() {
		return minPrice;
	}

	public void setMinPrice(float minPrice) {
		this.minPrice = minPrice;
	}

	public float getMaxPrice() {
		return maxPrice;
	}

	public void setMaxPrice(float maxPrice) {
		this.maxPrice = maxPrice;
	}

	public float getAvgVolume() {
		return avgVolume;
	}

	public void setAvgVolume(float avgVolume) {
		this.avgVolume = avgVolume;
	}
	
	
}
