package hadoop.ex1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.io.WritableComparable;

public class Ex1TupleWritable implements WritableComparable<Ex1TupleWritable>{


	private LocalDate date;
	
	private float close;
	
	private float low;
	
	private float high;
	
	private float volume;

	public Ex1TupleWritable() {
		
	}
	
	public Ex1TupleWritable(LocalDate date, float close, float low, float high, float volume) {
		this.date = date;
		this.close = close;
		this.low = low;
		this.high = high;
		this.volume = volume;
	}

	/*getters and setters*/
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

	
	
	
	
	/*implemented methods*/
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.date.toString());
		out.writeFloat(this.close);
		out.writeFloat(this.low);
		out.writeFloat(this.high);
		out.writeFloat(this.volume);
	}

	public void readFields(DataInput in) throws IOException {
		/*data*/
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		this.date = LocalDate.parse(in.readUTF(),formatter);
		
		/*close*/
		this.close = in.readFloat();
		
		/*low*/
		this.low = in.readFloat();
		
		/*high*/
		this.high = in.readFloat();
		
		/*volume*/
		this.volume = in.readFloat();
	}

	
	
	
	
	
	
	
	//*******************************************************************
	public int compareTo(Ex1TupleWritable j) {
		return this.date.compareTo(j.getDate());
	}	
	//*******************************************************************
	
	
	
	
	
	
	
	/*standards*/
	
//	@Override
//	public String toString() {
//		return "Job1TupleWritable [date=" + date + ", close=" + close + ", low=" + low + ", high=" + high + ", volume="
//				+ volume + "]";
//	}
	
	@Override
	public String toString() {
		return date.toString() + "" + close + " " + low + " " + high + " " + volume;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Float.floatToIntBits(close);
		result = prime * result + ((date == null) ? 0 : date.hashCode());
		result = prime * result + Float.floatToIntBits(high);
		result = prime * result + Float.floatToIntBits(low);
		result = prime * result + Float.floatToIntBits(volume);
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
		Ex1TupleWritable other = (Ex1TupleWritable) obj;
		if (Float.floatToIntBits(close) != Float.floatToIntBits(other.close))
			return false;
		if (date == null) {
			if (other.date != null)
				return false;
		} else if (!date.equals(other.date))
			return false;
		if (Float.floatToIntBits(high) != Float.floatToIntBits(other.high))
			return false;
		if (Float.floatToIntBits(low) != Float.floatToIntBits(other.low))
			return false;
		if (Float.floatToIntBits(volume) != Float.floatToIntBits(other.volume))
			return false;
		return true;
	}
}
