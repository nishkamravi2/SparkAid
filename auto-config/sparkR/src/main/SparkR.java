package sparkR.src.main;

import java.util.Hashtable;

public class SparkR {
	
	//SparkR
	static String rNumRBackendThreads = ""; //2

	public static void configureSparkRSettings(Hashtable<String, String> inputsTable,
			Hashtable<String, String> optionsTable,
			Hashtable<String, String> recommendationsTable,
			Hashtable<String, String> commandLineParamsTable) {
		
		setSparkR(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		
	}
	
	private static void setSparkR(Hashtable<String, String> inputsTable,
			Hashtable<String, String> optionsTable,
			Hashtable<String, String> recommendationsTable,
			Hashtable<String, String> commandLineParamsTable) {
		setRNumRBackendThreads(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		//add in more method/settings in the future
	}
	
	
	
	public static void setRNumRBackendThreads(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.r.numRBackendThreads", rNumRBackendThreads);
		recommendationsTable.put("spark.r.numRBackendThreads", "");
	}


}