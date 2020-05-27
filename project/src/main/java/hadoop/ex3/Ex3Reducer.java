package hadoop.ex3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utilities.Utilities;



/**
 * 
 * Reducer for Job2
 * 
 */
public class Ex3Reducer extends Reducer<Text,Text,Text,Text>{

	private static final String COMMA = ",";

	private Map<String,Integer> companiesNumbers;
	private Map<String,String> companiesQuotations;


	/**
	 * Set first record as the header with the names of the columns 
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		companiesNumbers = new HashMap<>();
		companiesQuotations = new HashMap<>();

		context.write(new Text("n. aziende trend comune" + COMMA + "{AZIENDE_CON_TREND_COMUNE}:" + COMMA) , new Text("2016: VAR_ANN_%" + COMMA + "2017: VAR_ANN_%" + COMMA + "2018: VAR_ANN_%"));
	}



	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

		String outputKey = "{";
		boolean isFirst = true;
		int numCompanies = 0;

		for(Text t : values) {
			if(isFirst) {
				outputKey += t;
				isFirst = false;
			}
			else
				outputKey += COMMA + t;
			numCompanies++;			
		}
		outputKey += "}:";

		/*insert only if there are at least two companies with same trend*/
		if(numCompanies > 1) {
			String yearsQuots = key.toString();
			this.companiesNumbers.put(outputKey, numCompanies);
			this.companiesQuotations.put(outputKey, yearsQuots);
		}
	}


	/**
	 * Use cleanup method to sort the map and produce the output result
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		this.companiesNumbers = Utilities.sortByValue(companiesNumbers);
		
		for(String comps : this.companiesNumbers.keySet()) {
			Text companiesText = new Text(comps);
			Text trend = new Text(this.companiesQuotations.get(comps));
			Integer numberOfCompanies = companiesNumbers.get(comps);
			//Text outputKey = new Text(companiesText.toString());

			/*also add numberOfCompanies to output for better readability*/
			Text outputKey = new Text(numberOfCompanies + COMMA + companiesText.toString());
			context.write(outputKey, trend);
		}
	}

}
