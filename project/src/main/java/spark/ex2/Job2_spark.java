package spark.ex2;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple6;
import scala.Tuple7;
import scala.Tuple9;
import utilities.Result_Ex1;
import utilities.Utilities;

public class Job2_spark {

	private static final String COMMA = ",";
	
	public static void main(String[] args) throws IOException {
		if (args.length!=3) {
			System.err.println("Usage: ./bin/spark-submit --class spark.job2.Job2_spark jar_path input_path/historical_stocks input_path/historical_stock_prices output_path");
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

		PairFunction<String,String,Tuple3<Float,Long,LocalDate>>prepareValuesHSP =
				line -> {
					String[] tokens = line.split(COMMA);
					
					String ticker = tokens[0];
					Float close = Float.parseFloat(tokens[2]);
					Long volume = Long.parseLong(tokens[6]);
					LocalDate date = LocalDate.parse(tokens[7]);
					
					return new Tuple2<>(ticker , new Tuple3<>(close,volume,date));
				};

		JavaPairRDD<String,Tuple2<String,String>> valuesHS = linesHS.filter(checkInputHS).mapToPair(prepareValuesHS);
		
		JavaPairRDD<String,Tuple3<Float,Long,LocalDate>> valuesHSP = linesHSP.filter(checkInputHSP).mapToPair(prepareValuesHSP);

		
		PairFunction<	Tuple2<String, Tuple2<Tuple3<Float, Long, LocalDate>, Tuple2<String, String>>>,					//<ticker, [(close,volume,date),(company,sector)]>
		String, Tuple9<Long,LocalDate,LocalDate,Float,Float,Float,Integer,String,String>> 	reorganizeValuesAfterJoin1 = 			//<(ticker,year),(volume,firstDate,lastDate,firstClose,lastClose,close,counterDays,company,sector)>
		tuple -> {
			String ticker = tuple._1;
			String year = tuple._2._1._3().getYear() + "";
			Long volume = tuple._2._1._2();
			LocalDate firstDate = tuple._2._1._3();
			LocalDate lastDate = tuple._2._1._3();
			Float firstClose = tuple._2._1._1();
			Float lastClose = tuple._2._1._1();
			Float close = tuple._2._1._1();
			Integer counterDays = 1;
			String company = tuple._2._2._1();
			String sector = tuple._2._2._2();

			String tickerYearKey = ticker + COMMA + year;

			return new Tuple2<>(tickerYearKey ,new Tuple9<>(volume,firstDate,lastDate,firstClose,lastClose,close,counterDays,company,sector));
		};
		
		//KEY: (ticker,year)	VALUE: (volume,firstDate,lastDate,firstClose,lastClose,close,counterDays,company,sector)>
		JavaPairRDD<String,Tuple9<Long,LocalDate,LocalDate,Float,Float,Float,Integer,String,String>> joinResults = valuesHSP.join(valuesHS).mapToPair(reorganizeValuesAfterJoin1);
						
		Function2<	Tuple9<Long,LocalDate,LocalDate,Float,Float,Float,Integer,String,String>,
					Tuple9<Long,LocalDate,LocalDate,Float,Float,Float,Integer,String,String>,
					Tuple9<Long,LocalDate,LocalDate,Float,Float,Float,Integer,String,String>> reduce_sumVolumesTicker_findFirstLastClose =
					(tuple1, tuple2) -> {
						Long sumVolume = tuple1._1() + tuple2._1();
						LocalDate firstDate = tuple1._2();
						LocalDate lastDate = tuple1._3();
						Float firstClose = tuple1._4();
						Float lastClose = tuple1._5();
						Float sumClose = tuple1._6() + tuple2._6();
						Integer counterDays = tuple1._7() + tuple2._7();
						String company = tuple1._8();
						String sector = tuple1._9();
						
						if(tuple2._2().isBefore(tuple1._2())) {
							firstDate = tuple2._2();
							firstClose = tuple2._4();
						}
						if(tuple2._3().isAfter(tuple1._3())) {
							lastDate = tuple2._3();
							lastClose = tuple2._5();
						}
						
						return new Tuple9<>(sumVolume,firstDate,lastDate,firstClose,lastClose,sumClose,counterDays,company,sector);
					};
					
		//change key KEY: (company,year) VALUE: (sumVolume,firstClose,lastClose,sumClose,counterDays,sector)>
		PairFunction<	Tuple2<String,Tuple9<Long,LocalDate,LocalDate,Float,Float,Float,Integer,String,String>>,
						String, Tuple6<Long,Float,Float,Float,Integer,String>> map_fromTickerToCompany = 
						tuple -> {
							String[] oldKey = tuple._1.split(COMMA);
							String ticker = oldKey[0];			//useless
							String year = oldKey[1];
							
							Long sumVolume = tuple._2._1();
							Float firstClose = tuple._2._4();
							Float lastClose = tuple._2._5();
							Float sumClose = tuple._2._6();
							Integer counterDays = tuple._2._7();
							String company = tuple._2._8();
							String sector = tuple._2._9();
							
							String companyYearKey = company + COMMA + year;
							return new Tuple2<>(companyYearKey, new Tuple6<>(sumVolume,firstClose,lastClose,sumClose,counterDays,sector));
						};
						
		JavaPairRDD<String, Tuple6<Long,Float,Float,Float,Integer,String>> aggregationOnTicker = joinResults.reduceByKey(reduce_sumVolumesTicker_findFirstLastClose)
																											.mapToPair(map_fromTickerToCompany);

		//(sumVolume,firstClose,lastClose,sumCloses,counterDays,sector)
		Function2<	Tuple6<Long,Float,Float,Float,Integer,String>,
					Tuple6<Long,Float,Float,Float,Integer,String>,
					Tuple6<Long,Float,Float,Float,Integer,String>> reduce_sumVolumeCompany_SumFirstLastCloseCompany_sumCloseSumCounterCompany = 
					(tuple1,tuple2) -> {
						Long sumVolumeCompany = tuple1._1() + tuple2._1();
						Float sumFirstCloses = tuple1._2() + tuple2._2();
						Float sumLastCloses = tuple1._3() + tuple2._3();
						Float sumCloses = tuple1._4() + tuple2._4();
						Integer sumCountersDays = tuple1._5() + tuple2._5();
						String sector = tuple1._6();
						
						return new Tuple6<>(sumVolumeCompany,sumFirstCloses,sumLastCloses,sumCloses,sumCountersDays,sector);
					};
		
					

		//KEY: (company,year) VALUE: (sumVolume,sumFirstCloses,sumLastCloses,sumCloses,sumCountersDays,sector)>
		//change key KEY: (sector,year)	VALUE:	(sumVolume,varYear,dailyQuot,counterCompanies)
		PairFunction<	Tuple2<String,Tuple6<Long,Float,Float,Float,Integer,String>>,
						String, Tuple4<Long,Float,Float,Integer>>	map_varYearCompany_dailyQuotCompany_fromCompanyToSector =
						tuple ->	{
							String[] oldKey = tuple._1.split(COMMA);
							String company = oldKey[0];			//useless
							String year = oldKey[1];
							
							Long sumVolume = tuple._2._1();
							
							Float sumFirstCloses = tuple._2._2();
							Float sumLastCloses = tuple._2._3();
							Float varYear = ((sumLastCloses - sumFirstCloses) / sumFirstCloses)*100;
							
							Float sumCloses = tuple._2._4();
							Integer sumCountersDays = tuple._2._5();
							Float dailyQuot = sumCloses / sumCountersDays;
							
							String sector = tuple._2._6();
							
							Integer counterCompanies = 1;
							
							String sectorYearKey = sector + COMMA + year;
							
							return new Tuple2<>(sectorYearKey, new Tuple4<>(sumVolume,varYear,dailyQuot,counterCompanies));
						};
						

		//(sumVolume,varYear,dailyQuot,counterCompanies)
		Function2<	Tuple4<Long,Float,Float,Integer>,
					Tuple4<Long,Float,Float,Integer>,
					Tuple4<Long,Float,Float,Integer>> reduce_sumVolumes_sumVars_sumQuots =
					(tuple1,tuple2) -> {
						Long sumVolumes = tuple1._1() + tuple2._1();
						Float sumVars = tuple1._2() + tuple2._2();
						Float sumQuots = tuple1._3() + tuple2._3();
						Integer counterCompanies = tuple1._4() + tuple2._4();
						
						return new Tuple4<>(sumVolumes,sumVars,sumQuots,counterCompanies);
					};
					
		//MAP TO COMPUTE RESULT
		Function<Tuple2<String, Tuple4<Long,Float,Float,Integer>>, String> map_avgSector =
				tuple -> {
					
					Integer counterCompanies = tuple._2._4();
					
					Long avgVolumes = tuple._2._1() / counterCompanies;
					
					Float avgVars = tuple._2._2() / counterCompanies;
					
					Float avgDailyQuots = tuple._2._3() / counterCompanies;
					
					String companyYear = tuple._1;
					
					return companyYear + COMMA + avgVolumes + COMMA + avgVars + "%" + COMMA + avgDailyQuots;
				};

				
				
				
		/**
		 * aggregationOnTicker
		 * reduce		reduce_sumVolumeCompany_SumFirstLastCloseCompany_sumCloseSumCounterCompany
		 * map			map_varYearCompany_dailyQuotCompany_fromCompanyToSector
		 * reduce		reduce_sumVolumes_sumVars_sumQuots
		 * map			map_avgSector
		 */
				
		JavaRDD<String> results = 	aggregationOnTicker.reduceByKey(reduce_sumVolumeCompany_SumFirstLastCloseCompany_sumCloseSumCounterCompany)
														.mapToPair(map_varYearCompany_dailyQuotCompany_fromCompanyToSector)
														.reduceByKey(reduce_sumVolumes_sumVars_sumQuots)
														.map(map_avgSector);
		
        List<String> listCsvLines = results.collect();
		
		FileWriter writer = new FileWriter(outputPath); 
		String header = "SETTORE" + COMMA + "ANNO" + COMMA + "VOLUME_ANNUALE_MEDIO" + COMMA + "VARIAZIONE_ANNUALE_MEDIA_%" + COMMA + "QUOTAZIONE_GIORNALIERA_MEDIA";

		writer.write(header + System.lineSeparator());
		
		for(String res: listCsvLines) {
        	writer.write(res + System.lineSeparator());
        }
		
		writer.close();

		session.stop();
		
		Instant finish = Instant.now();
		System.out.println("COMPUTING TIME: " + Duration.between(start, finish).toMillis());
	}

}
