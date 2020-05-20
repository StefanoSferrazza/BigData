package hadoop.ex3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utilities.Utilities;



/**
 * 
 * 
 * 
 * 
 *
 *
 *
 */
public class Ex3Reducer extends Reducer<Text,Text,Text,Text>{

	private static final String COMMA = ",";

	private Map<String,Integer> companiesNumbers = new HashMap<>();
	private Map<String,String> companiesQuotations = new HashMap<>();


	@Override
	protected void setup(Context context) throws IOException, InterruptedException{

		context.write(new Text("n. aziende trend comune" + COMMA + "{AZIENDE_CON_TREND_COMUNE}:") , new Text("2016: VAR_ANN_%" + COMMA + "2017: VAR_ANN_%" + COMMA + "2018: VAR_ANN_%"));
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

		if(numCompanies > 1) {
			String yearsQuots = key.toString();
			this.companiesNumbers.put(outputKey, numCompanies);
			this.companiesQuotations.put(outputKey, yearsQuots);
		}
	}



	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		this.companiesNumbers = Utilities.sortByValue(companiesNumbers);
		
		for(String comps : this.companiesNumbers.keySet()) {
			Text companiesText = new Text(comps);
			Text trend = new Text(this.companiesQuotations.get(comps));
			Integer numberOfCompanies = companiesNumbers.get(comps);
			Text outputKey = new Text(numberOfCompanies + COMMA + companiesText.toString());
			context.write(outputKey, trend);
		}
	}

}
