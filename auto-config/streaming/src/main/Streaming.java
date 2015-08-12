package streaming.src.main;

import java.util.Hashtable;

public class Streaming {
	public static void configureStreamingSettings(Hashtable<String, String> inputsTable,
			Hashtable<String, String> optionsTable,
			Hashtable<String, String> recommendationsTable,
			Hashtable<String, String> commandLineParamsTable) {
		setSparkStreaming(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	public static void setSparkStreaming(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		setStreamingBlockInterval(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}

	private static void setStreamingBlockInterval(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		recommendationsTable.put("spark.streaming.blockInterval", "Pick this value in such a way that (batch interval/block interval) = 2 * total-num-cores");
	}

}
