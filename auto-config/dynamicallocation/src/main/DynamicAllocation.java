package dynamicallocation.src.main;

import java.util.Hashtable;

public class DynamicAllocation {
	
	//Dynamic Allocation
	static String dynamicAllocationEnabled = "false"; //false
	static String shuffleServiceEnabled = "false";
	static String dynamicAllocationMaxExecutors = ""; //Integer.MAX_VALUE
	
	public static void configureDynamicAllocationSettings(Hashtable<String, String> inputsTable,
			Hashtable<String, String> optionsTable,
			Hashtable<String, String> recommendationsTable,
			Hashtable<String, String> commandLineParamsTable) {
		// Set Dynamic Allocation
		setDynamicAllocation(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	public static void setDynamicAllocation(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		setDynamicAllocationEnabled(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setDynamicAllocationMaxExecutors(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setShuffleServiceEnabled(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}

	private static void removeExecutorInstances(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.remove("spark.executor.instances");
	}
	
	private static void setDynamicAllocationEnabled(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		dynamicAllocationEnabled = "true";
		optionsTable.put("spark.dynamicAllocation.enabled", dynamicAllocationEnabled);
		recommendationsTable.put("spark.dynamicAllocation.enabled", "Note that cached data from decommissioned executors will be lost and might need recomputation, thereby affecting performance");
	}

	private static void setDynamicAllocationMaxExecutors(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		dynamicAllocationMaxExecutors = optionsTable.get("spark.executor.instances");
		optionsTable.put("spark.dynamicAllocation.maxExecutors", dynamicAllocationMaxExecutors);
		removeExecutorInstances(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	private static void setShuffleServiceEnabled(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		shuffleServiceEnabled = "true";
		optionsTable.put("spark.shuffle.service.enabled", shuffleServiceEnabled);
	}
	
	
}
