package dynamicallocation.src.main;

import java.util.Hashtable;

public class DynamicAllocation {
	
	//Dynamics Allocation
	static String dynamicAllocationEnabled = "false"; //false
	static String dynamicAllocationExecutorIdleTimeout = "60s";	//60s
	static String dynamicAllocationCachedExecutorIdleTimeout = ""; //2 * executorIdleTimeout
	static String initialExecutors = ""; //spark.dynamicAllocation.minExecutors
	static String dynamicAllocationMaxExecutors = ""; //Integer.MAX_VALUE
	static String dynamicAllocationMinExecutors = "0"; //0
	static String dynamicAllocationSchedulerBacklogTimeout = "1"; //1s
	static String dynamicAllocationSustainedSchedulerBackLogTimeout = ""; //schedulerBacklogTimeout
	static String shuffleServiceEnabled = "false";
	
	
	public static void configureDynamicAllocationSettings(Hashtable<String, String> inputsTable,
			Hashtable<String, String> optionsTable,
			Hashtable<String, String> recommendationsTable,
			Hashtable<String, String> commandLineParamsTable) {
		// Set Dynamic Allocation
		setDynamicAllocation(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	public static void setDynamicAllocation(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		setDynamicAllocationEnabled(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setDynamicAllocationExecutorIdleTimeout(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setDynamicAllocationCachedExecutorIdleTimeout(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setInitialExecutors(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setDynamicAllocationMaxExecutors(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setDynamicAllocationMinExecutors(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setDynamicAllocationSchedulerBacklogTimeout(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setDynamicAllocationSustainedSchedulerBackLogTimeout(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setShuffleServiceEnabled(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}

	private static void removeExecutorInstances(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.remove("spark.executor.instances");
	}
	
	private static void setDynamicAllocationEnabled(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		dynamicAllocationEnabled = "true";
		optionsTable.put("spark.dynamicAllocation.enabled", dynamicAllocationEnabled);
		recommendationsTable.put("spark.dynamicAllocation.enabled", "Note that cached data from decommissioned executors will be lost and might need recomputation, adding time");
	}

	private static void setDynamicAllocationExecutorIdleTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.executorIdleTimeout", dynamicAllocationExecutorIdleTimeout);
	}

	private static void setDynamicAllocationCachedExecutorIdleTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.cachedExecutorIdleTimeout", dynamicAllocationCachedExecutorIdleTimeout);
	}

	private static void setInitialExecutors(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.initialExecutors", initialExecutors);
	}

	private static void setDynamicAllocationMaxExecutors(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		dynamicAllocationMaxExecutors = optionsTable.get("spark.executor.instances");
		optionsTable.put("spark.dynamicAllocation.maxExecutors", dynamicAllocationMaxExecutors);
		removeExecutorInstances(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}

	private static void setDynamicAllocationMinExecutors(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.minExecutors", dynamicAllocationMinExecutors);
	}

	private static void setDynamicAllocationSchedulerBacklogTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.schedulerBacklogTimeout", dynamicAllocationSchedulerBacklogTimeout);
	}

	private static void setDynamicAllocationSustainedSchedulerBackLogTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", dynamicAllocationSustainedSchedulerBackLogTimeout);
	}
	
	private static void setShuffleServiceEnabled(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		shuffleServiceEnabled = "true";
		optionsTable.put("spark.shuffle.service.enabled", shuffleServiceEnabled);
	}
	
	
}
