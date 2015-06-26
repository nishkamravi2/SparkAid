package dynamicallocation.src.main;

import java.util.Hashtable;

public class DynamicAllocation {
	
	//Dynamics Allocation
	static String dynamicAllocationEnabled = ""; //false
	static String dynamicAllocationExecutorIdleTimeout = "";	//60s
	static String dynamicAllocationCachedExecutorIdleTimeout = ""; //2 * executorIdleTimeout
	static String initialExecutors = ""; //spark.dynamicAllocation.minExecutors
	static String dynamicAllocationMaxExecutors = ""; //Integer.MAX_VALUE
	static String dynamicAllocationMinExecutors = ""; //0
	static String dynamicAllocationSchedulerBacklogTimeout = ""; //1s
	static String dynamicAllocationSustainedSchedulerBackLogTimeout = ""; //schedulerBacklogTimeout
	
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
	}
	
	private static void setDynamicAllocationEnabled(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.enabled", dynamicAllocationEnabled);
		recommendationsTable.put("spark.dynamicAllocation.enabled", "");
	}

	private static void setDynamicAllocationExecutorIdleTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.executorIdleTimeout", dynamicAllocationExecutorIdleTimeout);
		recommendationsTable.put("spark.dynamicAllocation.executorIdleTimeout", "");
	}

	private static void setDynamicAllocationCachedExecutorIdleTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.cachedExecutorIdleTimeout", dynamicAllocationCachedExecutorIdleTimeout);
		recommendationsTable.put("spark.dynamicAllocation.cachedExecutorIdleTimeout", "");
	}

	private static void setInitialExecutors(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.initialExecutors", initialExecutors);
		recommendationsTable.put("spark.dynamicAllocation.initialExecutors", "");
	}

	private static void setDynamicAllocationMaxExecutors(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.maxExecutors", dynamicAllocationMaxExecutors);
		recommendationsTable.put("spark.dynamicAllocation.maxExecutors", "");
	}

	private static void setDynamicAllocationMinExecutors(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.minExecutors", dynamicAllocationMinExecutors);
		recommendationsTable.put("spark.dynamicAllocation.minExecutors", "");
	}

	private static void setDynamicAllocationSchedulerBacklogTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.schedulerBacklogTimeout", dynamicAllocationSchedulerBacklogTimeout);
		recommendationsTable.put("spark.dynamicAllocation.schedulerBacklogTimeout", "");
	}

	private static void setDynamicAllocationSustainedSchedulerBackLogTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", dynamicAllocationSustainedSchedulerBackLogTimeout);
		recommendationsTable.put("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", "");
	}
	
	
}
