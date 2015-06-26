package sparkR;

import java.util.Hashtable;

public class SparkR {
	
	//SparkR
	static String rNumRBackendThreads = ""; //2
	
	//Spark R
	public static void setRNumRBackendThreads(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.r.numRBackendThreads", rNumRBackendThreads);
		recommendationsTable.put("spark.r.numRBackendThreads", "default recommendation");
	}

	public static void configureSparkRSettings(String[] args,
			Hashtable<String, String> optionsTable,
			Hashtable<String, String> recommendationsTable,
			Hashtable<String, String> commandLineParamsTable) {
		
		setRNumRBackendThreads(args, optionsTable, recommendationsTable, commandLineParamsTable);
		
	}


}
