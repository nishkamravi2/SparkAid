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
	
	public static void configureDynamicAllocationSettings(String[] args,
			Hashtable<String, String> optionsTable,
			Hashtable<String, String> recommendationsTable,
			Hashtable<String, String> commandLineParamsTable) {
		// Set Dynamic Allocation
		setDynamicAllocation(args, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	
	public static void setDynamicAllocation(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		setDynamicAllocationEnabled(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setDynamicAllocationExecutorIdleTimeout(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setDynamicAllocationCachedExecutorIdleTimeout(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setInitialExecutors(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setDynamicAllocationMaxExecutors(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setDynamicAllocationMinExecutors(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setDynamicAllocationSchedulerBacklogTimeout(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setDynamicAllocationSustainedSchedulerBackLogTimeout(args, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	private static void setDynamicAllocationEnabled(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.enabled", dynamicAllocationEnabled);
		recommendationsTable.put("spark.dynamicAllocation.enabled", "default recommendation");
	}

	private static void setDynamicAllocationExecutorIdleTimeout(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.executorIdleTimeout", dynamicAllocationExecutorIdleTimeout);
		recommendationsTable.put("spark.dynamicAllocation.executorIdleTimeout", "default recommendation");
	}

	private static void setDynamicAllocationCachedExecutorIdleTimeout(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.cachedExecutorIdleTimeout", dynamicAllocationCachedExecutorIdleTimeout);
		recommendationsTable.put("spark.dynamicAllocation.cachedExecutorIdleTimeout", "default recommendation");
	}

	private static void setInitialExecutors(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.initialExecutors", initialExecutors);
		recommendationsTable.put("spark.dynamicAllocation.initialExecutors", "default recommendation");
	}

	private static void setDynamicAllocationMaxExecutors(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.maxExecutors", dynamicAllocationMaxExecutors);
		recommendationsTable.put("spark.dynamicAllocation.maxExecutors", "default recommendation");
	}

	private static void setDynamicAllocationMinExecutors(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.minExecutors", dynamicAllocationMinExecutors);
		recommendationsTable.put("spark.dynamicAllocation.minExecutors", "default recommendation");
	}

	private static void setDynamicAllocationSchedulerBacklogTimeout(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.schedulerBacklogTimeout", dynamicAllocationSchedulerBacklogTimeout);
		recommendationsTable.put("spark.dynamicAllocation.schedulerBacklogTimeout", "default recommendation");
	}

	private static void setDynamicAllocationSustainedSchedulerBackLogTimeout(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", dynamicAllocationSustainedSchedulerBackLogTimeout);
		recommendationsTable.put("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", "default recommendation");
	}
	
	
}
