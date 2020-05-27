hadoop-*/bin/hdfs dfs -rm -r output
hadoop-*/bin/hdfs dfs -rm -r temp
hadoop-*/bin/hdfs dfs -rm -r temp1
hadoop-*/bin/hdfs dfs -rm -r temp2

hadoop-*/bin/hadoop jar hadoop-3.2.1/BigDataFirstProject-1.0.jar hadoop/ex2_basic/Ex2 input/historical_stock_prices.csv input/historical_stocks.csv output/job2_Hadoop
