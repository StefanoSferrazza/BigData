package utilities;

import java.io.Serializable;


public class Result_Ex1 implements Comparable<Result_Ex1>,Serializable{

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

