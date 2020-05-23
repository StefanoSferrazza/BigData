package hadoop.ex2_basic;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 
 * Reducer for Job Join
 * 
 */
public class Ex2JoinReducer extends Reducer<Text, Text, Text, Text>{

	private static final String COMMA = ",";
	private static final String SEPARATOR_HS = "historical_stock";
	private static final String SEPARATOR_HSP = "historical_stock_prices";


	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

		/*maps initialization. each map is used to store a specific value with respect to a year (alternative to create an object, made to avoid too structures)*/
		HashMap<Integer, LocalDate> actionYearFirstDate = new HashMap<Integer, LocalDate>();
		HashMap<Integer, LocalDate> actionYearLastDate = new HashMap<Integer, LocalDate>();
		HashMap<Integer, Float> actionYearFirstClose = new HashMap<Integer, Float>();
		HashMap<Integer, Float> actionYearLastClose = new HashMap<Integer, Float>();
		HashMap<Integer, Long> actionYearSumVolume = new HashMap<Integer, Long>();
		HashMap<Integer, Float> actionYearSumDailyClose = new HashMap<Integer, Float>();
		HashMap<Integer, Long> actionYearNumRows = new HashMap<Integer, Long>();

		String sector = "";

		for(Text value : values) {
			String line = value.toString();
			String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

			if(tokens[0].equals(SEPARATOR_HS)) {
				sector = tokens[1];
			}

			else 
				if(tokens[0].equals(SEPARATOR_HSP)) {
					float close = Float.parseFloat(tokens[1]);
					long volume = Long.parseLong(tokens[2]);
					LocalDate date = LocalDate.parse(tokens[3]);
					int year = date.getYear();

					if(!actionYearFirstClose.containsKey(year)) {		//first actualYear initialization
						actionYearFirstDate.put(year,date);
						actionYearLastDate.put(year,date);
						actionYearFirstClose.put(year,close);
						actionYearLastClose.put(year,close);
						actionYearSumVolume.put(year,volume);
						actionYearSumDailyClose.put(year,close);
						actionYearNumRows.put(year,new Long(1));
					}
					else {										//already initialized
						/*update first-close*/
						if(date.isBefore(actionYearFirstDate.get(year))) {						
							actionYearFirstDate.replace(year,date);
							actionYearFirstClose.replace(year,close);
						}
						else
							/*update last-close*/
							if(date.isAfter(actionYearLastDate.get(year))) {
								actionYearLastDate.replace(year,date);						
								actionYearLastClose.replace(year,close);
							}

						/*update year-volume*/
						Long sumVolume = volume + actionYearSumVolume.get(year);
						actionYearSumVolume.replace(year,sumVolume);

						/*update daily-close*/
						Float sumDailyClose = close + actionYearSumDailyClose.get(year);
						actionYearSumDailyClose.replace(year,sumDailyClose);

						/*update counter-rows*/
						Long counterRows = 1 + actionYearNumRows.get(year);
						actionYearNumRows.put(year,counterRows);
					}
				}
		}

		if(!sector.equals("")) {	//required to remove cases without sector AFTER join
			for(Integer year : actionYearFirstClose.keySet()) {
				float lastClose = actionYearLastClose.get(year);
				float firstClose = actionYearFirstClose.get(year);
				/*calculate deltaQuotation based on its definition and round it*/
				float deltaQuotation = ((lastClose-firstClose)/firstClose)*100;

				long sumVolume = actionYearSumVolume.get(year);
				float sumDailyClose = actionYearSumDailyClose.get(year);
				long yearRow = actionYearNumRows.get(year);

				//	<(sector,year), (sumVolume,deltaQuotation,sumDailyClose,yearRow)>
				context.write(new Text(sector + COMMA + year), new Text(sumVolume + COMMA + deltaQuotation + COMMA + sumDailyClose + COMMA + yearRow));
			}
		}
	}
	
	
}
