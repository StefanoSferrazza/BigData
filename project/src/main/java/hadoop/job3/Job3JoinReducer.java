package hadoop.job3;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 
 * 
 * 
 *
 */
public class Job3JoinReducer extends Reducer<Text, Text, Text, FloatWritable>{

	private static final String COMMA = ",";
	private static final String SEPARATOR_HSP = "hsp";
	private static final String SEPARATOR_HS = "hs";


	/**
	 * 
	 * 
	 *
	 */
	private class Pair {
		public String companyName;
		public Integer year;

		public Pair(String companyName, Integer year) {
			this.companyName = companyName;
			this.year = year;
		}

		public boolean equals(Pair pair) {
			return this.companyName.equals(pair.companyName) &&
					this.year.equals(pair.year);
		}
	}


	private Map<Pair, Float> companyYearStartQuotation;
	private Map<Pair, Float> companyYearEndQuotation;
	private Map<Pair, Float> companyYearAnnualVariation;


	@Override
	protected void setup(Context context) {
		// 		super.setup(context); 										?????????????
		this.companyYearStartQuotation = new HashMap<Pair, Float>();
		this.companyYearEndQuotation = new HashMap<Pair, Float>();
	}



	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

		Map<Integer, Float> actionYearFirstClose = new HashMap<Integer, Float>();
		Map<Integer, Float> actionYearLastClose = new HashMap<Integer, Float>();
		Map<Integer, LocalDate>  actionYearFirstDate = new HashMap<Integer, LocalDate>();
		Map<Integer, LocalDate>  actionYearLastDate = new HashMap<Integer, LocalDate>();

		String companyName = "";

		for(Text value : values) {
			String line = value.toString();
			String[] tokens = line.split(COMMA);

			if(tokens[0].equals(SEPARATOR_HS)) {
				companyName = tokens[1];
			}
			else 
				if(tokens[0].equals(SEPARATOR_HSP)) {	
					float close = Float.parseFloat(tokens[1]);
					LocalDate date = LocalDate.parse(tokens[2]);
					int year = date.getYear();

					if(!actionYearFirstClose.containsKey(year)) {
						actionYearFirstDate.put(year,date);
						actionYearLastDate.put(year,date);
						actionYearFirstClose.put(year,close);
						actionYearLastClose.put(year,close);
					}
					else {
						if(date.isBefore(actionYearFirstDate.get(year))) {						
							actionYearFirstDate.replace(year,date);
							actionYearFirstClose.replace(year,close);
						}
						else
							if(date.isAfter(actionYearLastDate.get(year))) {
								actionYearLastDate.replace(year,date);						
								actionYearLastClose.replace(year,close);
							}
					}
				}
		}

		for(Integer year : actionYearFirstClose.keySet()) {
			float actionFirstClose = actionYearFirstClose.get(year);
			float actionLastClose = actionYearLastClose.get(year);
			
			Pair companyYearStartCloses = new Pair(companyName,year);
			Float startCloses = companyYearStartQuotation.get(companyYearStartCloses);
			if(startCloses != null)
				startCloses += actionFirstClose;
			else
				startCloses = actionFirstClose;
			this.companyYearStartQuotation.put(companyYearStartCloses, startCloses);

			Pair companyYearEndCloses = new Pair(companyName,year);
			Float endCloses = companyYearEndQuotation.get(companyYearEndCloses);
			if(endCloses != null)
				endCloses += actionLastClose;
			else
				endCloses = actionLastClose;
			this.companyYearEndQuotation.put(companyYearEndCloses, endCloses);
		}
	}




	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for(Pair companyYear : this.companyYearStartQuotation.keySet()) {
			float companyYearStartQuotation = this.companyYearStartQuotation.get(companyYear);
			float companyYearEndQuotation = this.companyYearEndQuotation.get(companyYear);

			float companyAnnualVariation = ((companyYearEndQuotation - companyYearStartQuotation)
					/companyYearStartQuotation)*100;

			this.companyYearAnnualVariation.put(companyYear, companyAnnualVariation);
		}
	}



}
