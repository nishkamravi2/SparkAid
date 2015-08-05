package streaming.src.main;

import java.util.Hashtable;

public class Streaming {

	//Spark Streaming
	static String streamingUnpersist = "true"; //true
	
	public static void configureStreamingSettings(Hashtable<String, String> inputsTable,
			Hashtable<String, String> optionsTable,
			Hashtable<String, String> recommendationsTable,
			Hashtable<String, String> commandLineParamsTable) {
		setSparkStreaming(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	public static void setSparkStreaming(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
	    setStreamingUnpersist(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}

	private static void setStreamingUnpersist(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.streaming.unpersist", streamingUnpersist);
		recommendationsTable.put("spark.streaming.unpersist", "Set to true if running streaming app and running into OOM issues");
	}

}
