package core.src.main.yarn;
import java.util.Hashtable;

import utils.UtilsConversion;

public class Yarn {
	
	//Heuristic Configured Parameters
	static String yarnAMMemory = ""; 
	static String yarnAMCores = ""; 
	static String executorInstances = ""; 
	static String yarnExecutorMemoryOverhead = ""; 
	static String yarnDriverMemoryOverhead = "";
	static String yarnAMMemoryOverhead = ""; 
	static String driverCores = ""; 
	static String coresMax = "";
	static String driverMemory = "";
	static String executorCores = "";
	
	//Variables for default Overhead Memory Setting, this is an inferred setting
	static double executorMemoryOverheadFraction = 0.10; //recommended by config guide
	static double driverMemoryOverheadFraction = 0.07; //recommended by config guide
	static double AMMemoryOverheadFraction = 0.07; //recommended by config guide
	
	//External variables not in Spark but used for configurations
	
	//System Overhead, OS + Resource Manager (e.g. CM) + other processes running in background
	static double systemOverheadBuffer = 1.00;
	static double systemOverheadBufferTier1 = 0.850;
	static double systemOverheadBufferTier2 = 0.900;
	static double systemOverheadBufferTier3 = 0.950;

	//Spark Overhead Buffer
	static double sparkOverheadBuffer = 1.00;
	static double sparkOverheadBufferTier1 = 0.800;
	static double sparkOverheadBufferTier2 = 0.850;
	static double sparkOverheadBufferTier3 = 0.900;
	
	static double driverMemorySafetyFraction = 0.90;
	static double executorUpperBoundLimitG = 64; //gb
	
	//Constants
	static double idealExecutorMemory = 8; //gb

	//YARN AM Defaults
	static String yarnAMExtraJavaOptions = "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"; //none
	static String executorExtraJavaOptions = "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps";
	static String driverExtraJavaOptions = "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps";
	static String schedulerMinRegisteredResourcesRatio = "0.8"; //Overrides Standalone's Default of 0.0
	
	public static void configureYarnSettings( Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		setYarnDefaults(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setExecMemCoresInstances(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setCoresMax(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setExecutorExtraJavaOptions(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		insertYarnNodeManagerCoresRecommendation(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		insertYarnSchedulerCoresRecommendation(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		
		setYarnAMMemory(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnAMMemoryOverhead(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnAMCores(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnAMExtraJavaOptions(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnDriverMemoryOverhead(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setDriverMemory(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable); //same as in standalone for now, will override standalone if different.
		setDriverCores(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable); //same as in standalone for now, will override standalone if different.
		setDriverExtraJavaOptions(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		
	}
	
	private static void setCoresMax(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		int allocateCoresMax = Integer.parseInt(executorCores) *Integer.parseInt(executorInstances);
		coresMax = Integer.toString(allocateCoresMax);
		optionsTable.put("spark.cores.max", coresMax);
	}

	public static void setYarnDefaults(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		setSchedulerMinRegisteredResourcesRatio(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	private static void insertYarnNodeManagerMemRecommendation (String recommendation, Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		recommendationsTable.put("environment - yarn.nodemanager.resource.memory-mb", "Yarn Mode Only: Recommended to set YARN Container: " + recommendation);
	}
	
	private static void insertYarnSchedulerMemRecommendation (String recommendation, Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		recommendationsTable.put("environment - yarn.scheduler.maximum-allocation-mb", "Yarn Mode Only: Recommended to set YARN Container: " + recommendation);
	}
	
	private static void insertYarnNodeManagerCoresRecommendation (Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		String recommendation = inputsTable.get("coresPerNode");
		recommendationsTable.put("environment - yarn.nodemanager.resource.cpu-vcores", "Yarn Mode Only: Number of virtual CPU cores that can be allocated for containers: " + recommendation);
	}
	
	private static void insertYarnSchedulerCoresRecommendation (Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		String recommendation = inputsTable.get("coresPerNode");
		recommendationsTable.put("environment - yarn.scheduler.maximum-allocation-vcores", "Yarn Mode Only: The largest number of virtual CPU cores that can be requested for a container: " + recommendation);
	}
	
	private static void setExecMemCoresInstances(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		double memoryPerNode = UtilsConversion.parseMemory(inputsTable.get("memoryPerNode")); //in mb
		int numNodes = Integer.parseInt(inputsTable.get("numNodes"));
		int numWorkerNodes = numNodes - 1;
		int numCoresPerNode = Integer.parseInt(inputsTable.get("numCoresPerNode"));
		
		double memoryPerWorkerNode = memoryPerNode;
		//Calculate the memory available for raw Spark
		if (memoryPerWorkerNode <= 50){
			systemOverheadBuffer = systemOverheadBufferTier1; 
			sparkOverheadBuffer = sparkOverheadBufferTier1;
		}
		else if (memoryPerWorkerNode <= 100){
			systemOverheadBuffer = systemOverheadBufferTier2;
			sparkOverheadBuffer = sparkOverheadBufferTier2;
		}
		else{
			systemOverheadBuffer = systemOverheadBufferTier3;
			sparkOverheadBuffer = sparkOverheadBufferTier3;
		}
		
		//YARN CONTAINER SIZE
		double rawSparkMemoryPerNode = memoryPerWorkerNode * systemOverheadBuffer * sparkOverheadBuffer; 
		
		insertYarnNodeManagerMemRecommendation (Integer.toString((int)rawSparkMemoryPerNode) + "g", inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		insertYarnSchedulerMemRecommendation (Integer.toString((int)rawSparkMemoryPerNode) + "g", inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);

		//User input of what fraction of resources of Spark cluster to be used
		double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
		double effectiveMemoryPerNode = rawSparkMemoryPerNode * resourceFraction;
		
		double driverMemoryValue = effectiveMemoryPerNode * driverMemorySafetyFraction;
		driverMemory = Integer.toString((int)driverMemoryValue) + "g";
		yarnAMMemory = driverMemory;
		yarnDriverMemoryOverhead = Integer.toString((int)(driverMemoryValue * driverMemoryOverheadFraction * 1000)); //mb
		yarnAMMemoryOverhead = Integer.toString((int)(driverMemoryValue * AMMemoryOverheadFraction * 1000)); //mb
		
		double idealExecutorMemoryYarnOverhead = idealExecutorMemory * executorMemoryOverheadFraction;
		double idealExecutorMemoryWithOverhead = idealExecutorMemory + idealExecutorMemoryYarnOverhead;
		
		int numExecutorsPerNode = 0;
		int calculatedNumExecutorsPerNode = (int)(effectiveMemoryPerNode / idealExecutorMemory);

		//Calculate Memory per Executor
		double finalExecutorMemory = idealExecutorMemoryWithOverhead;
		boolean recalculateFlag = false;
		if (calculatedNumExecutorsPerNode > 4){
			numExecutorsPerNode = 4;
			recalculateFlag = true;
		}
		else if (calculatedNumExecutorsPerNode < 2){
			numExecutorsPerNode = 2;
			recalculateFlag = true;
		}
		else{ //2 or 3 or 4
			numExecutorsPerNode = calculatedNumExecutorsPerNode;
			double currMemSizePerNode = idealExecutorMemory * numExecutorsPerNode;
			double leftOverMemPerNode = effectiveMemoryPerNode - currMemSizePerNode;
			if(leftOverMemPerNode > (idealExecutorMemory / 2)){
				recalculateFlag = true;
			}
		}
		
		//Re-adjusting Executor Memory
		if(recalculateFlag){
			 finalExecutorMemory = effectiveMemoryPerNode/numExecutorsPerNode;
		}
		
		finalExecutorMemory = Math.min(executorUpperBoundLimitG, finalExecutorMemory);
		double finalExecutorMemoryOverhead = finalExecutorMemory * executorMemoryOverheadFraction;

		//Calculate Cores Per Executor
		int effectiveCoresPerNode = (int) (resourceFraction * numCoresPerNode);
		int coresPerExecutor =  (int) (effectiveCoresPerNode / numExecutorsPerNode);
		executorCores = Integer.toString(coresPerExecutor);
	    setExecutorCores(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);	
		setExecutorMemory(Integer.toString((int)finalExecutorMemory) + "g", "", optionsTable, recommendationsTable, commandLineParamsTable);
		yarnExecutorMemoryOverhead = String.valueOf((int)(finalExecutorMemoryOverhead * 1000));
		setYarnExecutorMemoryOverhead (yarnExecutorMemoryOverhead, "",  optionsTable, recommendationsTable, commandLineParamsTable);
		
		int totalExecutorInstances =  numExecutorsPerNode * numWorkerNodes;
		setExecutorInstances (Integer.toString(totalExecutorInstances), "",  optionsTable, recommendationsTable, commandLineParamsTable);
		
	}
	private static void setDriverExtraJavaOptions(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		double memoryPerNode = Double.parseDouble(inputsTable.get("memoryPerNode"));
		if (memoryPerNode < 32){
			driverExtraJavaOptions += " -XX:+UseCompressedOops";
		}
		optionsTable.put("spark.driver.extraJavaOptions", driverExtraJavaOptions);
		recommendationsTable.put("spark.driver.extraJavaOptions", "In case of long gc pauses, try adding the following: -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled");
	}
	
	private static void setExecutorExtraJavaOptions(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		double memoryPerNode = Double.parseDouble(inputsTable.get("memoryPerNode"));
		if (memoryPerNode < 32){
			executorExtraJavaOptions += " -XX:+UseCompressedOops";
		}
		optionsTable.put("spark.executor.extraJavaOptions", executorExtraJavaOptions);
		recommendationsTable.put("spark.executor.extraJavaOptions", "In case of long gc pauses, try adding the following: -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled");
	}

	private static void setExecutorInstances (String value, String recommendation, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		executorInstances = value;
		optionsTable.put("spark.executor.instances", executorInstances);
		if (recommendation.length() > 0)
			recommendationsTable.put("spark.executor.instances", recommendation);
	}
	
	public static void setExecutorCores(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.executor.cores", executorCores);
	}

	private static void setExecutorMemory (String value, String recommendation, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.executor.memory", value);
		if (recommendation.length() > 0)
			recommendationsTable.put("spark.executor.memory", recommendation);
		
	}
	
	//Only modify this for Client mode, in cluster mode, use spark.driver.memory instead.
	public static void setYarnAMMemory(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){	
		optionsTable.put("spark.yarn.am.memory", yarnAMMemory);
	}
	
	//YARN AM Cluster mode
	//This will override the Standalone Driver Core settings if need be
	public static void setDriverCores(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
		String numCoresPerNode = inputsTable.get("numCoresPerNode");
		double effectiveDriverCores = resourceFraction * Double.parseDouble(numCoresPerNode);
		driverCores = Integer.toString((int)effectiveDriverCores);
		optionsTable.put("spark.driver.cores", driverCores);
		commandLineParamsTable.put("--driver-cores", driverCores);
	}
	
	//YARN AM Cluster mode
	//This will override the Standalone Driver Memory settings
	public static void setDriverMemory(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.driver.memory", driverMemory);
		commandLineParamsTable.put("--driver-memory", driverMemory);
	}
	
	//YARN AM Client mode
	public static void setYarnAMCores(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.am.cores", yarnAMCores);
		double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
		String numCoresPerNode = inputsTable.get("numCoresPerNode");
		double effectiveDriverCores = resourceFraction * Double.parseDouble(numCoresPerNode);
		yarnAMCores = Integer.toString((int)effectiveDriverCores);
		optionsTable.put("spark.yarn.am.cores", yarnAMCores);
	}

	//might need to delete this method if we follow heuristics as we calculate this for user instead
	public static void setYarnExecutorMemoryOverhead(String value, String recommendation, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.executor.memoryOverhead", value);
		if (recommendation.length() > 0) {
			recommendationsTable.put("spark.yarn.executor.memoryOverhead", "");
		}
	}

	public static void setYarnDriverMemoryOverhead(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
		double memoryPerNode = UtilsConversion.parseMemory(inputsTable.get("memoryPerNode")); //in mb
		double targetDriverMemory = memoryPerNode * driverMemorySafetyFraction * resourceFraction / (1 + driverMemoryOverheadFraction);
		double calculatedYarnDriverMemOverhead = targetDriverMemory * driverMemoryOverheadFraction;
		yarnDriverMemoryOverhead = Integer.toString((int)calculatedYarnDriverMemOverhead * 1024);
		optionsTable.put("spark.yarn.driver.memoryOverhead", yarnDriverMemoryOverhead);
	}

	//Same as spark.yarn.driver.memoryOverhead, but for the Application Master in client mode.
	public static void setYarnAMMemoryOverhead(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
		double memoryPerNode = UtilsConversion.parseMemory(inputsTable.get("memoryPerNode")); //in mb
		double targetDriverMemory = memoryPerNode * driverMemorySafetyFraction * resourceFraction / (1 + AMMemoryOverheadFraction);
		double calculatedYarnAMMemOverhead = targetDriverMemory * AMMemoryOverheadFraction;
		yarnAMMemoryOverhead = Integer.toString((int)calculatedYarnAMMemOverhead * 1024);
		optionsTable.put("spark.yarn.am.memoryOverhead", yarnAMMemoryOverhead);
	}

	//Works in Client Mode only
	public static void setYarnAMExtraJavaOptions(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		double memoryPerNode = Double.parseDouble(inputsTable.get("memoryPerNode"));
		if (memoryPerNode < 32){
			yarnAMExtraJavaOptions += " -XX:+UseCompressedOops";
		}
		optionsTable.put("spark.yarn.am.extraJavaOptions", yarnAMExtraJavaOptions);
		recommendationsTable.put("spark.yarn.am.extraJavaOptions", "In case of long gc pauses, try adding the following: -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled");
		optionsTable.remove("spark.driver.extraJavaOptions");
	}

	//Overrides Standalone Setting
	private static void setSchedulerMinRegisteredResourcesRatio(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.scheduler.minRegisteredResourcesRatio", schedulerMinRegisteredResourcesRatio);
	}
}
