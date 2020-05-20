package utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


/**
 * 
 * 
 * 
 *
 */
public class Utilities {
	
	public static float truncateToSecondDecimal(float number) {
		return ((float)Math.round(number*100))/100;
	}
	
	public static boolean inputExists(String input) {
		return !(input.isEmpty() || input.equals(" ") || input.equals("N/A") || input==null );
	}

	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort(Entry.comparingByValue());
        Collections.reverse(list);

        Map<K, V> result = new LinkedHashMap<>();
        for (Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

}

