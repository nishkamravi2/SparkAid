package streaming.src.main;

import java.util.Hashtable;

public class Streaming {

	//Spark Streaming
	static String streamingBlockInterval = ""; //200ms
	static String streamingReceiverMaxRate = ""; //not set
	static String streamingReceiverWriteAheadLogEnable = "";
	static String streamingUnpersist = ""; //true
	static String streamingKafkaMaxRatePerPartition = ""; //not set
	static String streamingKafkaMaxRetries = ""; //1
	static String streamingUIRetainedBatches = ""; //1000
	
	public static void configureStreamingSettings(String[] args,
			Hashtable<String, String> optionsTable,
			Hashtable<String, String> recommendationsTable,
			Hashtable<String, String> commandLineParamsTable) {
		
	}
	
	public static void setSparkStreaming(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		setStreamingBlockInterval(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setStreamingReceiverMaxRate(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setStreamingReceiverWriteAheadLogEnable(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setStreamingUnpersist(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setStreamingKafkaMaxRatePerPartition(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setStreamingKafkaMaxRetries(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setStreamingUIRetainedBatches(args, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	private static void setStreamingBlockInterval(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.streaming.blockInterval", streamingBlockInterval);
		recommendationsTable.put("spark.streaming.blockInterval", "default recommendation");
	}

	private static void setStreamingReceiverMaxRate(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.streaming.receiver.maxRate", streamingReceiverMaxRate);
		recommendationsTable.put("spark.streaming.receiver.maxRate", "default recommendation");
	}

	private static void setStreamingReceiverWriteAheadLogEnable(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.streaming.receiver.writeAheadLog.enable", streamingReceiverWriteAheadLogEnable);
		recommendationsTable.put("spark.streaming.receiver.writeAheadLog.enable", "default recommendation");
	}

	private static void setStreamingUnpersist(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.streaming.unpersist", streamingUnpersist);
		recommendationsTable.put("spark.streaming.unpersist", "default recommendation");
	}

	private static void setStreamingKafkaMaxRatePerPartition(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.streaming.kafka.maxRatePerPartition", streamingKafkaMaxRatePerPartition);
		recommendationsTable.put("spark.streaming.kafka.maxRatePerPartition", "default recommendation");
	}

	private static void setStreamingKafkaMaxRetries(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.streaming.kafka.maxRetries", streamingKafkaMaxRetries);
		recommendationsTable.put("spark.streaming.kafka.maxRetries", "default recommendation");
	}

	private static void setStreamingUIRetainedBatches(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.streaming.ui.retainedBatches", streamingUIRetainedBatches);
		recommendationsTable.put("spark.streaming.ui.retainedBatches", "default recommendation");
	}


}
