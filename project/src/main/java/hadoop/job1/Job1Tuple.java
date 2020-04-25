package hadoop.job1;

import java.util.Date;

public class Job1Tuple {

	private Date date;
	
	private float close;
	
	private float min;
	
	private float max;
	
	private float volume;

	public Job1Tuple(Date date, float close, float min, float max, float volume) {
		this.date = date;
		this.close = close;
		this.min = min;
		this.max = max;
		this.volume = volume;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public float getClose() {
		return close;
	}

	public void setClose(float close) {
		this.close = close;
	}

	public float getMin() {
		return min;
	}

	public void setMin(float min) {
		this.min = min;
	}

	public float getMax() {
		return max;
	}

	public void setMax(float max) {
		this.max = max;
	}

	public float getVolume() {
		return volume;
	}

	public void setVolume(float volume) {
		this.volume = volume;
	}	
	
}
