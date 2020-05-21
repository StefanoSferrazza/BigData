package hadoop.ex1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


/**
 * 
 * 
 * 
 * 
 *
 */
public class Ex1ResultWritable implements WritableComparable<Ex1ResultWritable>{

//	private String symbol;
	

	private float percentageChange;
	
	private float minPrice;
	
	private float maxPrice;
	
	private float avgVolume;

	public Ex1ResultWritable() {
		
	}
	
	public Ex1ResultWritable(
						float percentageChange, 
						float minPrice, 
						float maxPrice, 
						float avgVolume) {
		super();
		this.percentageChange = percentageChange;
		this.minPrice = minPrice;
		this.maxPrice = maxPrice;
		this.avgVolume = avgVolume;
	}

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

	public void write(DataOutput out) throws IOException {
		out.writeFloat(this.percentageChange);
		out.writeFloat(this.minPrice);
		out.writeFloat(this.maxPrice);
		out.writeFloat(this.avgVolume);
	}

	public void readFields(DataInput in) throws IOException {
		this.percentageChange = in.readFloat();
		this.minPrice = in.readFloat();
		this.maxPrice = in.readFloat();
		this.avgVolume = in.readFloat();
	}

	public int compareTo(Ex1ResultWritable o) {
		((Float)this.percentageChange).compareTo(o.getPercentageChange());
		return 0;
	}
	
//	@Override
//	public String toString() {
//		return "Job1ResultWritable [percentageChange=" + percentageChange + ", minPrice=" + minPrice + ", maxPrice="
//				+ maxPrice + ", avgVolume=" + avgVolume + "]";
//	}
	
	@Override
	public String toString() {
		return this.percentageChange + " " + this.minPrice + " " + this.maxPrice + " " + this.avgVolume;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Float.floatToIntBits(avgVolume);
		result = prime * result + Float.floatToIntBits(maxPrice);
		result = prime * result + Float.floatToIntBits(minPrice);
		result = prime * result + Float.floatToIntBits(percentageChange);
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
		Ex1ResultWritable other = (Ex1ResultWritable) obj;
		if (Float.floatToIntBits(avgVolume) != Float.floatToIntBits(other.avgVolume))
			return false;
		if (Float.floatToIntBits(maxPrice) != Float.floatToIntBits(other.maxPrice))
			return false;
		if (Float.floatToIntBits(minPrice) != Float.floatToIntBits(other.minPrice))
			return false;
		if (Float.floatToIntBits(percentageChange) != Float.floatToIntBits(other.percentageChange))
			return false;
		return true;
	}
	
}
