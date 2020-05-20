package hadoop.ex2_new;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class JoinReducer_withCompany extends Reducer<Text, Text, Text, Text>{

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
		HashMap<Integer, Long> yearRows = new HashMap<Integer, Long>();

		String sector = "";
		String company = "";

		for(Text value : values) {

			String line = value.toString();
			String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");


			if(tokens[0].equals(SEPARATOR_HS)) {
				company = tokens[1];
				sector = tokens[2];
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
					yearRows.put(year,new Long(1));
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

					Long counterRows = 1 + yearRows.get(year);
					yearRows.put(year,counterRows);
				}

			}
		}

		if(!sector.equals("")) {			//corrisponderebbe a dati non contenenti "sector" in "historical_stocks"
			for(Integer year : yearFirstClose.keySet()) {

				long sumVolume = yearSumVolume.get(year);
				
				float lastClose = yearLastClose.get(year);
				float firstClose = yearFirstClose.get(year);
//				float yearPercentageVariation = ((lastClose-firstClose)/firstClose)*100;
				
				float sumDailyClose = yearSumDailyClose.get(year);
				long yearRow = yearRows.get(year);
//				float avgDailyClose = sumDailyClose/yearRow;


				//	<(company,year), (sumVolume,lastClose,firstClose,sumDailyClose,yearRow,sector)>				
				context.write(new Text(company + COMMA + year), new Text(sumVolume + COMMA + lastClose + COMMA + firstClose + COMMA + sumDailyClose + COMMA + yearRow + COMMA + sector));
			}
		}
	}
}
