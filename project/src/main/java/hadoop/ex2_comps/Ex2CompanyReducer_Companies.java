package hadoop.ex2_comps;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 
 * Company Reducer for Job2
 * 
 */
public class Ex2CompanyReducer_Companies extends Reducer<Text,Text,Text,Text>{

	private static final String COMMA = ",";
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
		//	<(company,year), (sumVolume,lastClose,firstClose,sumDailyClose,yearRow,sector)>
		try {
			long companySumYearVolume = 0;
			float companySumLastCloses = 0;
			float companySumFirstCloses = 0;
			float companySumDailyCloses = 0;
			long companyYearRows = 0;
			
			for(Text value : values) {
				String line = value.toString();
				String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				
				if(tokens.length==5) {		//6) {
					companySumYearVolume += Long.parseLong(tokens[0]);
					companySumLastCloses += Float.parseFloat(tokens[1]);
					companySumFirstCloses += Float.parseFloat(tokens[2]);
					companySumDailyCloses += Float.parseFloat(tokens[3]);
					companyYearRows += Long.parseLong(tokens[4]);
				}
			}

			/*calculate companyDeltaQuotation based on its definition*/
			float companyDeltaQuotation = ((companySumLastCloses-companySumFirstCloses)/companySumFirstCloses)*100;
			/*calculate companyAvgDailyClose based on its definition*/
			float companyAvgDailyClose = companySumDailyCloses/companyYearRows;
			
			String[] keys = key.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			
//			<(sector,year), (companySumYearVolume,companyDeltaQuotation,companyAvgDailyClose)>
			context.write(new Text(keys[1] + COMMA + keys[2]), new Text(companySumYearVolume + COMMA + companyDeltaQuotation + COMMA + companyAvgDailyClose));// + COMMA + 1));		
		}
		catch(Exception e) {
			e.printStackTrace();
			return;
		}
	}
}
