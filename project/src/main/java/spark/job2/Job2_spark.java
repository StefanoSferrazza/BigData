package spark.job2;

import java.time.Instant;
import java.time.LocalDate;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import utilities.Utilities;

public class Job2_spark {

	private static final String COMMA = ",";
	
	public static void main(String[] args) {
		if (args.length!=3) {
			System.err.println("Usage: ./bin/spark-submit --class spark.job1.Job1_spark jar_path input_path/historical_stocks input_path/historical_stock_prices output_path");
			System.exit(1);
		}

		String inputPathHS = args[0];
		String inputPathHSP = args[1];
		String outputPath = args[2];

		Instant start = Instant.now();


		SparkSession session = SparkSession
				.builder()
				.appName("Job2")
				.getOrCreate();

		JavaRDD<String> linesHS = session.read().textFile(inputPathHS).javaRDD();

		JavaRDD<String> linesHSP = session.read().textFile(inputPathHSP).javaRDD();


		Function<String,Boolean> checkInputHS = 
				line ->	{
							try {
								String[] tokens = line.split(COMMA);
								if(tokens.length==5 &&
										Utilities.inputExists(tokens[0]) &&		//ticker
										Utilities.inputExists(tokens[2]) &&		//azienda
										Utilities.inputExists(tokens[3])) {		//settore
									return true;
								}
								else return false;
							}
							catch(Exception e) {
								return false;
							}
						};

		Function<String,Boolean> checkInputHSP = 
				line ->	{
							try {
								String[] tokens = line.split(COMMA);
								if(tokens.length==8 &&
										Utilities.inputExists(tokens[0]) &&		//ticker
										Utilities.inputExists(tokens[2]) &&		//close
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

		PairFunction<String, String, Tuple2<String,String>> prepareValuesHS = 
				line -> {
					String[] tokens = line.split(COMMA);
					
					String ticker = tokens[0];
					String company = tokens[2];
					String sector = tokens[3];
					
					return new Tuple2<>(ticker,new Tuple2<>(company,sector));
				};

		PairFunction<String,String,Tuple4<Float,Long,LocalDate,Integer>>prepareValuesHSP =
				line -> {
					String[] tokens = line.split(COMMA);
					
					String ticker = tokens[0];
					Float close = Float.parseFloat(tokens[2]);
					Long volume = Long.parseLong(tokens[6]);
					LocalDate date = LocalDate.parse(tokens[7]);
					Integer counter = 1;
					
					return new Tuple2<>(ticker , new Tuple4<>(close,volume,date,counter));
				};

		JavaPairRDD<String,Tuple2<String,String>> valuesHS = linesHS.filter(checkInputHS).mapToPair(prepareValuesHS);
		
		JavaPairRDD<String,Tuple4<Float,Long,LocalDate,Integer>> valuesHSP = linesHSP.filter(checkInputHSP).mapToPair(prepareValuesHSP);

		 JavaPairRDD<String, Tuple2<Tuple4<Float, Long, LocalDate, Integer>, Tuple2<String, String>>> result = valuesHSP.join(valuesHS);

	}

}
