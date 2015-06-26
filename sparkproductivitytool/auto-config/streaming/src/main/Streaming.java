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
	
	public static void configureStreamingSettings(Hashtable<String, String> inputsTable,
			Hashtable<String, String> optionsTable,
			Hashtable<String, String> recommendationsTable,
			Hashtable<String, String> commandLineParamsTable) {
		setSparkStreaming(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	public static void setSparkStreaming(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		setStreamingBlockInterval(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setStreamingReceiverMaxRate(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setStreamingReceiverWriteAheadLogEnable(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setStreamingUnpersist(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setStreamingKafkaMaxRatePerPartition(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setStreamingKafkaMaxRetries(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setStreamingUIRetainedBatches(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	private static void setStreamingBlockInterval(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.streaming.blockInterval", streamingBlockInterval);
		recommendationsTable.put("spark.streaming.blockInterval", "");
	}

	private static void setStreamingReceiverMaxRate(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.streaming.receiver.maxRate", streamingReceiverMaxRate);
		recommendationsTable.put("spark.streaming.receiver.maxRate", "");
	}

	private static void setStreamingReceiverWriteAheadLogEnable(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.streaming.receiver.writeAheadLog.enable", streamingReceiverWriteAheadLogEnable);
		recommendationsTable.put("spark.streaming.receiver.writeAheadLog.enable", "");
	}

	private static void setStreamingUnpersist(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.streaming.unpersist", streamingUnpersist);
		recommendationsTable.put("spark.streaming.unpersist", "");
	}

	private static void setStreamingKafkaMaxRatePerPartition(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.streaming.kafka.maxRatePerPartition", streamingKafkaMaxRatePerPartition);
		recommendationsTable.put("spark.streaming.kafka.maxRatePerPartition", "");
	}

	private static void setStreamingKafkaMaxRetries(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.streaming.kafka.maxRetries", streamingKafkaMaxRetries);
		recommendationsTable.put("spark.streaming.kafka.maxRetries", "");
	}

	private static void setStreamingUIRetainedBatches(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.streaming.ui.retainedBatches", streamingUIRetainedBatches);
		recommendationsTable.put("spark.streaming.ui.retainedBatches", "");
	}


}
