package utilities;

public class Utilities {
	public float truncateToSecondDecimal(float number) {
		return ((float)Math.round(number*100))/100;
	}
}
