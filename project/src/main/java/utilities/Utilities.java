package utilities;

public class Utilities {
	
	public static float truncateToSecondDecimal(float number) {
		return ((float)Math.round(number*100))/100;
	}
	
	public static boolean inputExists(String input) {
		return !(input.isEmpty() || input.equals(" ") || input.equals("N/A") || input==null );
	}
}
