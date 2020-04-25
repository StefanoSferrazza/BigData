package hadoop.job1;

import java.time.LocalDate;

public class Job1Tuple {

	private LocalDate date;
	
	private float close;
	
	private float low;
	
	private float high;
	
	private float volume;

	public Job1Tuple(LocalDate date, float close, float low, float high, float volume) {
		this.date = date;
		this.close = close;
		this.low = low;
		this.high = high;
		this.volume = volume;
	}

	public LocalDate getDate() {
		return date;
	}

	public void setDate(LocalDate date) {
		this.date = date;
	}

	public float getClose() {
		return close;
	}

	public void setClose(float close) {
		this.close = close;
	}

	public float getLow() {
		return low;
	}

	public void setLow(float low) {
		this.low = low;
	}

	public float getHigh() {
		return high;
	}

	public void setHigh(float high) {
		this.high = high;
	}

	public float getVolume() {
		return volume;
	}

	public void setVolume(float volume) {
		this.volume = volume;
	}	
	
}
