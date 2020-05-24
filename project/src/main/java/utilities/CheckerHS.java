package utilities;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

/**
 *  Questa classe mira alla pulizia del dataset "Historical_stocks.csv" da eventuali dati sporchi o assenti. Viene fatto per tutti i campi che vengono utilizzati
 *  dai vari job sviluppati, ignorando eventuali dati sporchi non rilevanti.
 */
public class CheckerHS {
	
	public static void main(String[] args) throws IOException {

		String inputPath = args[0];
		String outputPath = args[1];
		
		SparkSession session = SparkSession
				.builder()
				.appName("CheckerHS")
				.getOrCreate();
		
		Function<String,Boolean> checkInputHS = 
				line ->	{
					try {
						String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
						if(tokens.length==5 &&
								Utilities.inputExists(tokens[0]) &&		//ticker
//								Utilities.inputExists(tokens[1]) &&		//exchange
								Utilities.inputExists(tokens[2]) &&		//azienda
								Utilities.inputExists(tokens[3])// &&	//settore
								/*Utilities.inputExists(tokens[4])*/) {	//industria
							return true;
						}
						else return false;
					}
					catch(Exception e) {
						return false;
					}
		};
		
		JavaRDD<String> results = session.read().textFile(inputPath).javaRDD().filter(checkInputHS);
	
		List<String> filtered = results.collect();
		
		FileWriter writer = new FileWriter(outputPath); 

		for(String line: filtered) {
        	writer.write(line + System.lineSeparator());
        }
		
		writer.close();

		session.stop();
	}
}
