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

	public static void configureSQLSettings(String[] args,
			Hashtable<String, String> optionsTable,
			Hashtable<String, String> recommendationsTable,
			Hashtable<String, String> commandLineParamsTable) {
		setSQL(args, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	private static void setSQL(String[] args,
			Hashtable<String, String> optionsTable,
			Hashtable<String, String> recommendationsTable,
			Hashtable<String, String> commandLineParamsTable) {
		
		//Tuning
		setSQLInMemoryColumnarStorageCompressed(args, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLInMemoryColumnarStorageBatchSize(args, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLAutoBroadcastJoinThreshold(args, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLCodegen(args, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLShufflePartitions(args, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLPlannerExternalSort(args, optionsTable, recommendationsTable, commandLineParamsTable);
		
		//Parquet
		setSQLParquetBinaryAsString(args, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLParquetInt96AsTimestamp(args, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLParquetCacheMetadata(args, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLParquetCompressionCodec(args, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLParquetFilterPushdown(args, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLHiveConvertMetastoreParquet(args, optionsTable, recommendationsTable, commandLineParamsTable);
		
		//Hive
		setSQLHiveMetastoreVersion(args, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLHiveMetastoreJars(args, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLHiveMetastoreSharedPrefixes(args, optionsTable, recommendationsTable, commandLineParamsTable);
		setSQLHiveMetastoreBarrierPrefixes(args, optionsTable, recommendationsTable, commandLineParamsTable);
		
	}


	//SQL Tuning
	public static void setSQLInMemoryColumnarStorageCompressed(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.inMemoryColumnarStorage.compressed", SQLInMemoryColumnarStorageCompressed);
		recommendationsTable.put("spark.sql.inMemoryColumnarStorage.compressed", "default recommendation");
	}

	public static void setSQLInMemoryColumnarStorageBatchSize(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.inMemoryColumnarStorage.batchSize", SQLInMemoryColumnarStorageBatchSize);
		recommendationsTable.put("spark.sql.inMemoryColumnarStorage.batchSize", "default recommendation");
	}

	public static void setSQLAutoBroadcastJoinThreshold(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.autoBroadcastJoinThreshold", SQLAutoBroadcastJoinThreshold);
		recommendationsTable.put("spark.sql.autoBroadcastJoinThreshold", "default recommendation");
	}

	public static void setSQLCodegen(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.codegen", SQLCodegen);
		recommendationsTable.put("spark.sql.codegen", "default recommendation");
	}

	public static void setSQLShufflePartitions(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.shuffle.partitions", SQLShufflePartitions);
		recommendationsTable.put("spark.sql.shuffle.partitions", "default recommendation");
	}

	public static void setSQLPlannerExternalSort(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.planner.externalSort", SQLPlannerExternalSort);
		recommendationsTable.put("spark.sql.planner.externalSort", "default recommendation");
	}
	
	//Parquet
	
	public static void setSQLParquetBinaryAsString(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.parquet.binaryAsString", SQLParquetBinaryAsString);
		recommendationsTable.put("spark.sql.parquet.binaryAsString", "default recommendation");
	}

	public static void setSQLParquetInt96AsTimestamp(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.parquet.int96AsTimestamp", SQLParquetInt96AsTimestamp);
		recommendationsTable.put("spark.sql.parquet.int96AsTimestamp", "default recommendation");
	}

	public static void setSQLParquetCacheMetadata(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.parquet.cacheMetadata", SQLParquetCacheMetadata);
		recommendationsTable.put("spark.sql.parquet.cacheMetadata", "default recommendation");
	}

	public static void setSQLParquetCompressionCodec(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.parquet.compression.codec", SQLParquetCompressionCodec);
		recommendationsTable.put("spark.sql.parquet.compression.codec", "default recommendation");
	}

	public static void setSQLParquetFilterPushdown(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.parquet.filterPushdown", SQLParquetFilterPushdown);
		recommendationsTable.put("spark.sql.parquet.filterPushdown", "default recommendation");
	}

	public static void setSQLHiveConvertMetastoreParquet(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.hive.convertMetastoreParquet", SQLHiveConvertMetastoreParquet);
		recommendationsTable.put("spark.sql.hive.convertMetastoreParquet", "default recommendation");
	}
	
	//Hive
	public static void setSQLHiveMetastoreVersion(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.hive.metastore.version", SQLHiveMetastoreVersion);
		recommendationsTable.put("spark.sql.hive.metastore.version", "default recommendation");
	}
	
	public static void setSQLHiveMetastoreJars(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.hive.metastore.jars", SQLHiveMetastoreVersion);
		recommendationsTable.put("spark.sql.hive.metastore.jars", "default recommendation");
	}

	public static void setSQLHiveMetastoreSharedPrefixes(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.hive.metastore.sharedPrefixes", SQLHiveMetastoreSharedPrefixes);
		recommendationsTable.put("spark.sql.hive.metastore.sharedPrefixes", "default recommendation");
	}

	public static void setSQLHiveMetastoreBarrierPrefixes(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.sql.hive.metastore.barrierPrefixes", SQLHiveMetastoreBarrierPrefixes);
		recommendationsTable.put("spark.sql.hive.metastore.barrierPrefixes", "default recommendation");
	}

}
