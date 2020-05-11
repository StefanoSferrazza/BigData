package spark.job1;

import java.io.FileWriter;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.Tuple8;
import utilities.Result_Job1;
import utilities.Utilities;

public class Job1_spark {

	private static final String COMMA = ",";

	public static void main(String[] args) throws Exception {

		if (args.length!=2) {
			System.err.println("Usage: ./bin/spark-submit --class spark.job1.Job1_spark jar_path input_path output_path");
			System.exit(1);
		}
		
		String inputPath = args[0];
		String outputPath = args[1];

//		String inputPath= "/home/bigdata/Documenti/bigData/progetto/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv";
//		String outputPath = "/home/bigdata/Scrivania/prova/prova.csv";

		Instant start = Instant.now();
		
		// Spark Session creation

		SparkSession session = SparkSession
				.builder()
				.appName("Job1")
				.getOrCreate();

		// Import and Map Creation
		JavaRDD<String> lines = session.read().textFile(inputPath).javaRDD();


		Function<String,Boolean> checkLine = s ->	{
														try {
															String[] tokens = s.split(COMMA);
															if(tokens.length==8 &&
																	Utilities.inputExists(tokens[0]) &&
																	Utilities.inputExists(tokens[2]) &&
																	Utilities.inputExists(tokens[6]) &&
																	Utilities.inputExists(tokens[7])) {
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
												
		
		//<ticker, (firstDate,lastDate,firstClose,lastClose,minClose,maxClose,volume,counter)>																						
		PairFunction<String, String, Tuple8<LocalDate,LocalDate,Float,Float,Float,Float,Long,Integer>> prepareValues = 
				s -> {
					String[] tokens = s.split(COMMA);
					
					String ticker = tokens[0];
					LocalDate date = LocalDate.parse(tokens[7]);
					Float close = Float.parseFloat(tokens[2]);
					Long volume = Long.parseLong(tokens[6]);
					Integer counter = 1;
					return new Tuple2<>(ticker, new Tuple8<>(date,date,close,close,close,close,volume,counter));
				};
		

		JavaPairRDD<String,Tuple8<LocalDate,LocalDate,Float,Float,Float,Float,Long,Integer>> tuplesRedundant = lines.filter(checkLine)
																											.mapToPair(prepareValues);
		
		// Reduce creation
		Function2<Tuple8<LocalDate, LocalDate, Float, Float, Float, Float, Long, Integer>, Tuple8<LocalDate, LocalDate, Float, Float, Float, Float, Long, Integer>, Tuple8<LocalDate, LocalDate, Float, Float, Float, Float, Long, Integer>> reducer = 
				(t1,t2) -> {
					LocalDate firstDate = t1._1();
					LocalDate lastDate = t1._2();
					Float firstClose = t1._3();
					Float lastClose = t1._4();
					Float minClose = t1._5();
					Float maxClose = t1._6();
					Long volume = t1._7() + t2._7();
					Integer counter = t1._8() + t2._8();
					if(t2._1().isBefore(t1._1())) {
						firstDate = t2._1();
						firstClose = t2._3();
					}
					if(t2._2().isAfter(t1._2())) {
						lastDate = t2._2();
						lastClose = t2._4();
					}
					if(t2._5()<t1._5()) {
						minClose = t2._5();
					}
					if(t2._6()>t1._6()) {
						maxClose = t2._6();
					}
					return new Tuple8<>(firstDate,lastDate,firstClose,lastClose,minClose,maxClose,volume,counter);
				};
		
		
		JavaPairRDD<String,Tuple8<LocalDate,LocalDate,Float,Float,Float,Float,Long,Integer>> resultsReduntant = tuplesRedundant.reduceByKey(reducer);
		
		//PRODUCE RESULTS
		
		Function<Tuple2<String, Tuple8<LocalDate,LocalDate,Float,Float,Float,Float,Long,Integer>>, Result_Job1> produceResults =
				t -> {
					
					String ticker = t._1();
					
					float firstClose = t._2()._3();
					
					float lastClose = t._2()._4();
					
					float percentageChange = ((lastClose - firstClose) / firstClose)*100;
					percentageChange = Utilities.truncateToSecondDecimal(percentageChange);

					float minPrice = t._2()._5();
					
					float maxPrice = t._2()._6();
					
					long sumVolume = t._2()._7();
					long counter = t._2()._8();
					long avgVolume = sumVolume / counter;

					return new Result_Job1(ticker, percentageChange, minPrice, maxPrice, avgVolume);
					
				};
				
				//percentageChange, ticker + COMMA + percentageChange + COMMA + minPrice + COMMA + maxPrice + COMMA + avgVolume
				
				/*PER ORDINARE PER ORDINE DECRESCENTE USA sortByKey([ascending], [numTasks])   
				 * FACENDO RESTITUIRE PRIMA DELLE COPPIE CHIAVE VALORE DOVE LA CHIAVE È LA VARIAZIONE CON IL MENO DAVANTI*/
		
		JavaRDD<Result_Job1> results = resultsReduntant.map(produceResults).sortBy(f -> f, true, 1);
		
        List<Result_Job1> listCsvLines = results.collect();
        
        
        /*SCRIVE IN OUTPUT*/
        
        
        FileWriter writer = new FileWriter(outputPath); 
        
        String header = "TICKER" + COMMA + "VARIAZIONE_QUOTAZIONE_%" + COMMA + "PREZZO_MIN" + COMMA + "PREZZO_MAX" + COMMA + "VOLUME_MEDIO";
        writer.write(header + System.lineSeparator());
        
        for(Result_Job1 res: listCsvLines) {
        	writer.write(res.getTicker() + COMMA + res.toString() + System.lineSeparator());
        }
        writer.close();

		session.stop();
		
		Instant finish = Instant.now();
		System.out.println("COMPUTING TIME: " + Duration.between(start, finish).toMillis());
	}
	
}
