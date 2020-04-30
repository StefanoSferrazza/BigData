package hadoop.job2;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReduce2 extends Reducer<Text, Text, Text, Text>{
	private static final String COMMA = ",";
	private static final String SEPARATOR_HS = "historical_stock";
	private static final String SEPARATOR_HSP = "historical_stock_prices";


	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
		/*maps initialization. each map is used to store a specific value respecting into a year (alternative to create an object, made to avoid too structures)*/
		HashMap<Integer, Float> yearFirstClose = new HashMap<Integer, Float>();
		HashMap<Integer, Float> yearLastClose = new HashMap<Integer, Float>();
		HashMap<Integer, Long> yearSumVolume = new HashMap<Integer, Long>();
		HashMap<Integer, LocalDate>  yearFirstDate = new HashMap<Integer, LocalDate>();
		HashMap<Integer, LocalDate>  yearLastDate = new HashMap<Integer, LocalDate>();
		HashMap<Integer, Float> yearSumDailyClose = new HashMap<Integer, Float>();
		HashMap<Integer, Integer> yearRows = new HashMap<Integer, Integer>();
		
		String sector = "";
		
		for(Text value : values) {

			String line = value.toString();
			String[] tokens = line.split(COMMA);
			
			if(tokens[0].equals(SEPARATOR_HS)) {
				sector = tokens[1];
			}
			
			else if(tokens[0].equals(SEPARATOR_HSP)) {
				
				float close = Float.parseFloat(tokens[1]);
				long volume = Long.parseLong(tokens[2]);
				LocalDate date = LocalDate.parse(tokens[3]);
				int year = date.getYear();
				
				if(!yearFirstClose.containsKey(year)) {		//first actualYear initialization
					yearFirstClose.put(year,close);
					yearLastClose.put(year,close);
					yearSumVolume.put(year,volume);
					yearFirstDate.put(year,date);
					yearLastDate.put(year,date);
					yearSumDailyClose.put(year,close);
					yearRows.put(year,1);
				}
				else {										//already initialized
					/*update first close*/
					if(date.isBefore(yearFirstDate.get(year))) {						
						yearFirstDate.replace(year,date);
						yearFirstClose.replace(year,close);
					}
					
					/*update last close*/
					if(date.isAfter(yearLastDate.get(year))) {
						yearLastDate.replace(year,date);						
						yearLastClose.replace(year,close);
					}
					
					/*update year volume*/
					Long sumVolume = volume + yearSumVolume.get(year);
					yearSumVolume.replace(year,sumVolume);
					
					/*update */
					Float sumDailyClose = close + yearSumDailyClose.get(year);
					yearSumDailyClose.replace(year,sumDailyClose);
					
					Integer counterRows = 1 + yearRows.get(year);
					yearRows.put(year,counterRows);
				}
				
			}
		}
		
		for(Integer year : yearFirstClose.keySet()) {
			
			float sumDailyClose = yearSumDailyClose.get(year);
			float yearRow = yearRows.get(year);
			float avgDailyClose = sumDailyClose/yearRow;

			float lastClose = yearLastClose.get(year);
			float firstClose = yearFirstClose.get(year);
			float yearPercentageVariation = ((lastClose-firstClose)/firstClose)*100;
			
			Long sumVolume = yearSumVolume.get(year);
			
			//	<(ticker,year), (sector,avgDailyClose,yearPercentageVariation,sumVolume)>
			context.write(new Text(key + COMMA + year), new Text(sector + COMMA + avgDailyClose + COMMA + yearPercentageVariation + COMMA + sumVolume));
		}
	}
}