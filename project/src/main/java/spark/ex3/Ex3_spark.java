package spark.ex3;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple1;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;
import scala.Tuple6;
import scala.Tuple9;
import utilities.Utilities;

public class Ex3_spark {

	private static final String COMMA = ",";

	private static final String SEMICOLON =";";
	
	public static void main(String[] args) throws IOException {

		if (args.length!=3) {
			System.err.println("Usage: ./bin/spark-submit --class spark.job3.Job3_spark jar_path input_path/historical_stocks input_path/historical_stock_prices output_path");
			System.exit(1);
		}

		String inputPathHS = args[0];
		String inputPathHSP = args[1];
		String outputPath = args[2];

		SparkSession session = SparkSession
				.builder()
				.appName("Job3")
				.getOrCreate();

		JavaRDD<String> linesHS = session.read().textFile(inputPathHS).javaRDD();

		JavaRDD<String> linesHSP = session.read().textFile(inputPathHSP).javaRDD();


		Function<String,Boolean> checkInputHS = 
				line ->	{
					try {
						String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
						if(tokens.length==5 &&
								Utilities.inputExists(tokens[0]) &&		//ticker
								Utilities.inputExists(tokens[2])) {		//azienda
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
								Utilities.inputExists(tokens[7])) {		//date
							Float close = Float.parseFloat(tokens[2]);
							LocalDate date = LocalDate.parse(tokens[7]);
							if(date.getYear()>=2016 && date.getYear()<=2018 ) {
								return true;
							}
						}
						return false;
					}
					catch(Exception e) {
						return false;
					}
		};

		PairFunction<String, String, String> prepareValuesHS = 
				line -> {
					String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

					String ticker = tokens[0];
					String company = tokens[2];

					return new Tuple2<>(ticker,company);
		};

		PairFunction<String,String,Tuple2<Float,LocalDate>>prepareValuesHSP =
				line -> {
					String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

					String ticker = tokens[0];
					Float close = Float.parseFloat(tokens[2]);
					LocalDate date = LocalDate.parse(tokens[7]);

					return new Tuple2<>(ticker , new Tuple2<>(close,date));
		};
		
		
		
		PairFunction<	Tuple2<String, Tuple2<Tuple2<Float, LocalDate>, String>>,					//<ticker, [(close,date),(company)]>
		String, Tuple5<LocalDate,LocalDate,Float,Float,String>> 	reorganizeValuesAfterJoin = 			//<(ticker,year),(firstDate,lastDate,firstClose,lastClose,company)>
		tuple -> {
			String ticker = tuple._1;
			String year = tuple._2._1._2().getYear() + "";
			LocalDate firstDate = tuple._2._1._2();
			LocalDate lastDate = tuple._2._1._2();
			Float firstClose = tuple._2._1._1();
			Float lastClose = tuple._2._1._1();
			String company = tuple._2._2;

			String tickerYearKey = ticker + COMMA + year;

			return new Tuple2<>(tickerYearKey ,new Tuple5<>(firstDate,lastDate,firstClose,lastClose,company));
		};
		
		//(firstDate,lastDate,firstClose,lastClose,company)
		Function2<	Tuple5<LocalDate,LocalDate,Float,Float,String>,
					Tuple5<LocalDate,LocalDate,Float,Float,String>,
					Tuple5<LocalDate,LocalDate,Float,Float,String> > reduce_findFirstLastCloses =
					(tuple1,tuple2) -> {
						
						LocalDate firstDate = tuple1._1();
						LocalDate lastDate = tuple1._2();
						Float firstClose = tuple1._3();
						Float lastClose = tuple1._4();
						String company = tuple1._5();
						
						if(tuple2._1().isBefore(tuple1._1())) {
							firstDate = tuple2._1();
							firstClose = tuple2._3();
						}
						if(tuple2._2().isAfter(tuple1._2())) {
							lastDate = tuple2._2();
							lastClose = tuple2._4();
						}
						
						return new Tuple5<>(firstDate,lastDate,firstClose,lastClose,company);
		};
		
		
		
		PairFunction<	Tuple2<String,Tuple5<LocalDate,LocalDate,Float,Float,String>>,
						String, Tuple2<Float,Float>> map_fromTickerToCompany = 
		tuple -> {
			String[] oldKey = tuple._1.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			String ticker = oldKey[0];			//useless
			String year = oldKey[1];
			
			Float firstClose = tuple._2._3();
			Float lastClose = tuple._2._4();
			
			String company = tuple._2._5();
			
			String companyYearKey = company + COMMA + year;
			return new Tuple2<>(companyYearKey, new Tuple2<>(firstClose,lastClose));
		};
		
		
		
		//firstClose,lastClose
		Function2<	Tuple2<Float,Float>,
					Tuple2<Float,Float>,
					Tuple2<Float,Float> > reduce_sumFirstLastCloses =
					(tuple1,tuple2) -> {
						
						Float sumFirstCloses = tuple1._1() + tuple2._1();
						Float sumLastCloses = tuple1._2() + tuple2._2();
						
						return new Tuple2<>(sumFirstCloses,sumLastCloses);
		};
		
		
		PairFunction<	Tuple2<String, Tuple2<Float,Float>>,
		String, Tuple3<Integer,Integer,Integer>> map_calculateVarPercCompanyYear_changeKeyToCompany =
			tuple -> {
		
				String[] oldKey = tuple._1.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				String company = oldKey[0];			
				Integer year = Integer.parseInt(oldKey[1]);

				Float sumFirstCloses = tuple._2._1();
				Float sumLastCloses = tuple._2._2();

				Integer varYear = (int)(((sumLastCloses - sumFirstCloses) / sumFirstCloses)*100);

				Integer varYear2016 = null;
				Integer varYear2017 = null;
				Integer varYear2018 = null;

				if(year==2016)
					varYear2016 = varYear;
				if(year==2017)
					varYear2017 = varYear;
				if(year==2018)
					varYear2018 = varYear;

				return new Tuple2<>(company, new Tuple3<>(varYear2016,varYear2017,varYear2018));

		};


		Function2<	Tuple3<Integer,Integer,Integer>,
					Tuple3<Integer,Integer,Integer>,
					Tuple3<Integer,Integer,Integer> > reduce_unifyTrends =
				(tuple1,tuple2) -> {
		
					Integer varYear2016 = tuple1._1();
					Integer varYear2017 = tuple1._2();
					Integer varYear2018 = tuple1._3();

					if(varYear2016==null) {
						varYear2016=tuple2._1();
					}
					if(varYear2017==null) {
						varYear2017=tuple2._2();
					}
					if(varYear2018==null) {
						varYear2018=tuple2._3();
					}

					return new Tuple3<>(varYear2016,varYear2017,varYear2018);
		};

		Function<Tuple2<String,Tuple3<Integer,Integer,Integer>>,Boolean> checkAllYearPresent = 
				tuple -> {
					if(		tuple._2._1()==null || 
							tuple._2._2()==null || 
							tuple._2._3()==null) {
						return false;
					}
					else
						return true;
		};
		
		PairFunction<	Tuple2<String,Tuple3<Integer,Integer,Integer>>,
		Tuple3<Integer,Integer,Integer>, String> invertKey_fromCompany_toVarYear =
			tuple -> {
//				Integer varYear2016 = Math.round(tuple._2()._1());
//				Integer varYear2017 = Math.round(tuple._2()._2());
//				Integer varYear2018 = Math.round(tuple._2()._3());
				
				Integer varYear2016 = tuple._2()._1();
				Integer varYear2017 = tuple._2()._2();
				Integer varYear2018 = tuple._2()._3();
				
				String company = tuple._1();
				return new Tuple2<>(new Tuple3<>(varYear2016,varYear2017,varYear2018), company);
		};
		
		Function2<	String,
					String,
					String	> reduce_companySameTrend =
					(tuple1, tuple2) -> {
						return tuple1 + COMMA + tuple2;
		};
		
		
		PairFunction<	Tuple2<Tuple3<Integer,Integer,Integer>, String>,
						Integer,
						Tuple2<Tuple3<Integer,Integer,Integer>, String>> mapByNumberSimilarCompaniesTrend =
						tuple	->	{
							Integer similarities = tuple._2.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").length;
							return new Tuple2<>(similarities,tuple);
						};
		
		
		JavaPairRDD<String,String> valuesHS = linesHS.filter(checkInputHS).mapToPair(prepareValuesHS);
		
		JavaPairRDD<String,Tuple2<Float,LocalDate>> valuesHSP = linesHSP.filter(checkInputHSP).mapToPair(prepareValuesHSP);
		
		
		//context.write(new Text( " { AZIENDE_CON_TREND_COMUNE }:") , new Text("2016: VAR_ANN_%" + COMMA + "2017: VAR_ANN_%" + COMMA + "2018: VAR_ANN_%"));
		
		JavaPairRDD<Integer,Tuple2<Tuple3<Integer,Integer,Integer>, String>> results = valuesHSP.join(valuesHS)
																				.mapToPair(reorganizeValuesAfterJoin)
																				.reduceByKey(reduce_findFirstLastCloses)
																				.mapToPair(map_fromTickerToCompany)
																				.reduceByKey(reduce_sumFirstLastCloses)
																				.mapToPair(map_calculateVarPercCompanyYear_changeKeyToCompany)
																				.reduceByKey(reduce_unifyTrends)
																				.filter(checkAllYearPresent)
																				.mapToPair(invertKey_fromCompany_toVarYear)
																				.reduceByKey(reduce_companySameTrend)
																				.mapToPair(mapByNumberSimilarCompaniesTrend)
																				.sortByKey(false);
		
		
		
		FileWriter writer = new FileWriter(outputPath); 
		String header = "n. aziende trend comune" + COMMA + "{AZIENDE_CON_TREND_COMUNE}:" + COMMA + "2016: VAR_ANN_%" + COMMA + "2017: VAR_ANN_%" + COMMA + "2018: VAR_ANN_%";
		
		writer.write(header + System.lineSeparator());

		for(Tuple2<Integer,Tuple2<Tuple3<Integer,Integer,Integer>, String>> res : results.collect()) {
			Integer numCompaniesSimilarTrend = res._1;
			if(numCompaniesSimilarTrend>1) {
				String companies = res._2._2();
				Integer varYear2016 = res._2._1._1();
				Integer varYear2017 = res._2._1._2();
				Integer varYear2018 = res._2._1._3();
				writer.write(numCompaniesSimilarTrend + COMMA + "{" + companies + "}: " + COMMA + "2016: " + varYear2016 + "%" + COMMA + "2017: " + varYear2017 + "%" + COMMA + "2018: " + varYear2018 + "%" + System.lineSeparator());
			}
		}
		
		writer.close();

		session.stop();
		

	}
}
