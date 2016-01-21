package dynamicallocation.src.main;

import java.util.Hashtable;

public class DynamicAllocation {
	
	//Dynamic Allocation
	static String dynamicAllocationEnabled = "false"; //false
	static String shuffleServiceEnabled = "false";
	static String dynamicAllocationMaxExecutors = ""; //Integer.MAX_VALUE
	static String dynamicAllocationMinExecutors = "0"; 
	static String schedulerMaxRegisteredResourcesWaitingTime = "30000";
	
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
	    setDynamicAllocationMinExecutors(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setShuffleServiceEnabled(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setSchedulerMaxRegisteredResourcesWaitingTime(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
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
		int maxExecutors = Integer.parseInt(optionsTable.get("spark.executor.instances"));
		maxExecutors = (int)(maxExecutors * 1.5);
		dynamicAllocationMaxExecutors = maxExecutors + "";
		optionsTable.put("spark.dynamicAllocation.maxExecutors", dynamicAllocationMaxExecutors);
		removeExecutorInstances(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}

	private static void setDynamicAllocationMinExecutors(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		dynamicAllocationMinExecutors = "1";
		optionsTable.put("spark.dynamicAllocation.minExecutors", dynamicAllocationMinExecutors);
	}
	
	private static void setShuffleServiceEnabled(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		shuffleServiceEnabled = "true";
		optionsTable.put("spark.shuffle.service.enabled", shuffleServiceEnabled);
	}
	
	private static void setSchedulerMaxRegisteredResourcesWaitingTime(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.scheduler.maxRegisteredResourcesWaitingTime", schedulerMaxRegisteredResourcesWaitingTime);
	}
	
}
