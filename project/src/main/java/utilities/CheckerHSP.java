package utilities;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

/**
 *  Questa classe mira alla pulizia del dataset "Historical_stock_prices.csv" da eventuali dati sporchi o assenti. Viene fatto per tutti i campi che vengono utilizzati
 *  dai vari job sviluppati, nell'intervallo di date richiesto, ignorando eventuali dati sporchi che non siano rilevanti.
 */
public class CheckerHSP {
	
	private static final String COMMA = ",";
		
	public static void main(String[] args) throws IOException {

		String inputPath = args[0];
		String outputPath = args[1];
		
		SparkSession session = SparkSession
				.builder()
				.appName("CheckerHSP")
				.getOrCreate();
		
		Function<String,Boolean> checkInputHSP = 
				line ->	{
							try {
								String[] tokens = line.split(COMMA);
								if(tokens.length==8 &&
										Utilities.inputExists(tokens[0]) &&		//ticker
//										Utilities.inputExists(tokens[1]) &&		//open
										Utilities.inputExists(tokens[2]) &&		//close
//										Utilities.inputExists(tokens[3]) &&		//adj_close
//										Utilities.inputExists(tokens[4]) &&		//low
//										Utilities.inputExists(tokens[5]) &&		//high
										Utilities.inputExists(tokens[6]) &&		//volume
										Utilities.inputExists(tokens[7])) {		//date
									Float close = Float.parseFloat(tokens[2]);
									Long volume = Long.parseLong(tokens[6]);
									LocalDate date = LocalDate.parse(tokens[7]);
									if(date.getYear()>=2008 && date.getYear()<=2018 ) {
										return true;
									}
								}
								return false;
							}
							catch(Exception e) {
								return false;
							}
		};
		
		JavaRDD<String> results = session.read().textFile(inputPath).javaRDD().filter(checkInputHSP);
	
		List<String> filtered = results.collect();
		
		FileWriter writer = new FileWriter(outputPath); 

		for(String line: filtered) {
        	writer.write(line + System.lineSeparator());
        }
		
		writer.close();

		session.stop();
	}
}
