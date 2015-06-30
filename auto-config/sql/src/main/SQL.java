package sql.src.main;

import java.util.Hashtable;

public class SQL {
	
	//SQL Tuning
	static String SQLInMemoryColumnarStorageCompressed = ""; //true
	static String SQLInMemoryColumnarStorageBatchSize = ""; //10000
	static String SQLAutoBroadcastJoinThreshold = ""; //10485760
	static String SQLCodegen = ""; //false
	static String SQLShufflePartitions = ""; //200
	static String SQLPlannerExternalSort = ""; //false
	
	//SQL Parquet
	static String SQLParquetBinaryAsString = ""; //false
	static String SQLParquetInt96AsTimestamp = ""; //true
	static String SQLParquetCacheMetadata = ""; //true
	static String SQLParquetCompressionCodec = ""; //gzip
	static String SQLParquetFilterPushdown = ""; //false
	static String SQLHiveConvertMetastoreParquet = ""; //true
	
	//SQL Hive
	static String SQLHiveMetastoreVersion = ""; //0.13.1
	static String SQLHiveMetastoreJars = ""; //builtin
	static String SQLHiveMetastoreSharedPrefixes = ""; //com.mysql.jdbc, org.postgresql, com.microsoft.sqlserver, oracle.jdbc
	static String SQLHiveMetastoreBarrierPrefixes = ""; //empty

	public static void configureSQLSettings(Hashtable<String, String> inputsTable,
			Hashtable<String, String> optionsTable,
			Hashtable<String, String> recommendationsTable,
			Hashtable<String, String> commandLineParamsTable) {
		setSQL(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	private static void setSQL(Hashtable<String, String> inputsTable,
			Hashtable<String, String> optionsTable,
			Hashtable<String, String> recommendationsTable,
			Hashtable<String, String> commandLineParamsTable) {
		
		//Tuning
		setSQLInMemoryColumnarStorageCompressed(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLInMemoryColumnarStorageBatchSize(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLAutoBroadcastJoinThreshold(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLCodegen(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLShufflePartitions(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLPlannerExternalSort(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		
		//Parquet
		setSQLParquetBinaryAsString(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLParquetInt96AsTimestamp(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLParquetCacheMetadata(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLParquetCompressionCodec(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLParquetFilterPushdown(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLHiveConvertMetastoreParquet(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		
		//Hive
		setSQLHiveMetastoreVersion(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLHiveMetastoreJars(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLHiveMetastoreSharedPrefixes(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLHiveMetastoreBarrierPrefixes(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		
	}


	//SQL Tuning
	public static void setSQLInMemoryColumnarStorageCompressed(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.inMemoryColumnarStorage.compressed", SQLInMemoryColumnarStorageCompressed);
		recommendationsTable.put("spark.sql.inMemoryColumnarStorage.compressed", "");
	}

	public static void setSQLInMemoryColumnarStorageBatchSize(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.inMemoryColumnarStorage.batchSize", SQLInMemoryColumnarStorageBatchSize);
		recommendationsTable.put("spark.sql.inMemoryColumnarStorage.batchSize", "");
	}

	public static void setSQLAutoBroadcastJoinThreshold(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.autoBroadcastJoinThreshold", SQLAutoBroadcastJoinThreshold);
		recommendationsTable.put("spark.sql.autoBroadcastJoinThreshold", "");
	}

	public static void setSQLCodegen(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.codegen", SQLCodegen);
		recommendationsTable.put("spark.sql.codegen", "");
	}

	public static void setSQLShufflePartitions(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.shuffle.partitions", SQLShufflePartitions);
		recommendationsTable.put("spark.sql.shuffle.partitions", "");
	}

	public static void setSQLPlannerExternalSort(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.planner.externalSort", SQLPlannerExternalSort);
		recommendationsTable.put("spark.sql.planner.externalSort", "");
	}
	
	//Parquet
	
	public static void setSQLParquetBinaryAsString(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.parquet.binaryAsString", SQLParquetBinaryAsString);
		recommendationsTable.put("spark.sql.parquet.binaryAsString", "");
	}

	public static void setSQLParquetInt96AsTimestamp(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.parquet.int96AsTimestamp", SQLParquetInt96AsTimestamp);
		recommendationsTable.put("spark.sql.parquet.int96AsTimestamp", "");
	}

	public static void setSQLParquetCacheMetadata(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.parquet.cacheMetadata", SQLParquetCacheMetadata);
		recommendationsTable.put("spark.sql.parquet.cacheMetadata", "");
	}

	public static void setSQLParquetCompressionCodec(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.parquet.compression.codec", SQLParquetCompressionCodec);
		recommendationsTable.put("spark.sql.parquet.compression.codec", "");
	}

	public static void setSQLParquetFilterPushdown(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.parquet.filterPushdown", SQLParquetFilterPushdown);
		recommendationsTable.put("spark.sql.parquet.filterPushdown", "");
	}

	public static void setSQLHiveConvertMetastoreParquet(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.hive.convertMetastoreParquet", SQLHiveConvertMetastoreParquet);
		recommendationsTable.put("spark.sql.hive.convertMetastoreParquet", "");
	}
	
	//Hive
	public static void setSQLHiveMetastoreVersion(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.hive.metastore.version", SQLHiveMetastoreVersion);
		recommendationsTable.put("spark.sql.hive.metastore.version", "");
	}
	
	public static void setSQLHiveMetastoreJars(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.hive.metastore.jars", SQLHiveMetastoreVersion);
		recommendationsTable.put("spark.sql.hive.metastore.jars", "");
	}

	public static void setSQLHiveMetastoreSharedPrefixes(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.hive.metastore.sharedPrefixes", SQLHiveMetastoreSharedPrefixes);
		recommendationsTable.put("spark.sql.hive.metastore.sharedPrefixes", "");
	}

	public static void setSQLHiveMetastoreBarrierPrefixes(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.hive.metastore.barrierPrefixes", SQLHiveMetastoreBarrierPrefixes);
		recommendationsTable.put("spark.sql.hive.metastore.barrierPrefixes", "");
	}

}