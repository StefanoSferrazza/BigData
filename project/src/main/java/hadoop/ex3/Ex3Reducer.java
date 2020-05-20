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

	private Map<String,Integer> numCompanies = new HashMap<>();
	private Map<String,Text> companiesQuotations = new HashMap<>();


	@Override
	protected void setup(Context context) throws IOException, InterruptedException{

		context.write(new Text( " { COMPANIES_COMMON_TREND }:") , new Text("2016: ANN_VAR%" + COMMA + "2017: ANN_VAR%" + COMMA + "2018: ANN_VAR%"));
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

		String outputKey = "{";
		boolean isFirst = true;
		int numbCompanies = 0;

		for(Text t : values) {
			if(isFirst) {
				outputKey += t;
				isFirst = false;
			}
			else
				outputKey += COMMA + t;
			numbCompanies++;			
		}
		outputKey += "}:";

		if(numbCompanies > 1) {
			this.numCompanies.put(outputKey, numbCompanies);
			this.companiesQuotations.put(outputKey, key);
		}
	}



	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		this.numCompanies = Utilities.sortByValue(numCompanies);
		
		for(String comps : this.numCompanies.keySet()) {
			Text outputKey = new Text(comps);
			Text trend = this.companiesQuotations.get(comps);
			context.write(outputKey, trend);
		}
	}

}
