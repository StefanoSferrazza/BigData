package hadoop.ex3;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 
 * 
 * 
 *
 */
public class Ex3JoinReducer extends Reducer<Text, Text, Text, Text>{

	private static final String COMMA = ",";
	private static final String SEPARATOR_HSP = "hsp";
	private static final String SEPARATOR_HS = "hs";
	private static final String COLON = ":";


	/**
	 * 
	 * 
	 *
	 */
	private class Pair implements Comparable<Pair>{
		public String companyName;
		public Integer year;

		public Pair(String companyName, Integer year) {
			this.companyName = companyName;
			this.year = year;
		}


		@Override
		public boolean equals(Object obj) {
			Pair pair = (Pair) obj;
			return this.companyName.equals(pair.companyName) &&
					this.year.equals(pair.year);
		}

		@Override
		public int hashCode() {
			return this.companyName.hashCode() + this.year.hashCode();
		}


		public int compareTo(Pair p) {								// !!!!!!!!!!!!!!!! DO BETTER 
			// with COMPARATOR
			int i = companyName.compareTo(p.companyName);
			if (i != 0) 
				return i;
			return Integer.compare(year, p.year);
		}
	}



	private Map<Pair, Float> companyYearStartQuotation;
	private Map<Pair, Float> companyYearEndQuotation;
	private Map<String, String> companyAnnualVariations;


	@Override
	protected void setup(Context context) {
		// 		super.setup(context); 							?????????????
		this.companyYearStartQuotation = new TreeMap<Pair, Float>();
		this.companyYearEndQuotation = new TreeMap<Pair, Float>();
		this.companyAnnualVariations = new HashMap<String, String>();
	}



	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

		Map<Integer, LocalDate> actionYearFirstDate = new HashMap<Integer, LocalDate>();
		Map<Integer, LocalDate> actionYearLastDate = new HashMap<Integer, LocalDate>();
		Map<Integer, Float> actionYearFirstClose = new HashMap<Integer, Float>();
		Map<Integer, Float> actionYearLastClose = new HashMap<Integer, Float>();

		String companyName = "";

		for(Text value : values) {
			String line = value.toString();
			String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

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
			Float startCloses = this.companyYearStartQuotation.get(companyYearStartCloses);
			if(startCloses != null)
				startCloses += actionFirstClose;
			else
				startCloses = actionFirstClose;
			this.companyYearStartQuotation.put(companyYearStartCloses, startCloses);

			Pair companyYearEndCloses = new Pair(companyName,year);
			Float endCloses = this.companyYearEndQuotation.get(companyYearEndCloses);
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

			int companyAnnualVariation = (int) (((companyYearEndQuotation - companyYearStartQuotation)
					/companyYearStartQuotation)*100);



			//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! CONVERT INTO INTEGER



			String annualVariations = this.companyAnnualVariations.get(companyYear.companyName);
			if(annualVariations != null)
				annualVariations += COMMA + companyYear.year + COLON + companyAnnualVariation + "%";
			else
				annualVariations = companyYear.year + COLON + companyAnnualVariation + "%";
			this.companyAnnualVariations.put(companyYear.companyName, annualVariations);
		}

		for(String companyName : this.companyAnnualVariations.keySet()) {	
			context.write(new Text(companyName),
					new Text(this.companyAnnualVariations.get(companyName)));	
		}
	}



}
