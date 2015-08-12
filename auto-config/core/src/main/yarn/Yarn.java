package core.src.main.yarn;
import java.util.Hashtable;

import core.src.main.common.Common;
import utils.src.main.UtilsConversion;

public class Yarn {
	
	//Heuristic Configured Parameters
	static String yarnExecutorMemoryOverhead = ""; 
	static String yarnDriverMemoryOverhead = "";
	
	//YARN AM Defaults
	static String schedulerMinRegisteredResourcesRatio = "0.8"; //Overrides Standalone's Default of 0.0
	
	//Variables for default Overhead Memory Setting, this is an inferred setting
	static double executorMemoryOverheadFraction = 0.10; //recommended by config guide
	static double driverMemoryOverheadFraction = 0.07; //recommended by config guide

	public static void configureYarnSettings( Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		setExecMemCoresInstances(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		Common.setCoresMax(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		insertYarnNodeManagerCoresRecommendation(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		insertYarnSchedulerCoresRecommendation(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setSchedulerMinRegisteredResourcesRatio(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}

	private static void setExecMemCoresInstances(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
		double memoryPerNode = UtilsConversion.parseMemory(inputsTable.get("memoryPerNode")); //in mb
		int numNodes = Integer.parseInt(inputsTable.get("numNodes"));
		int numJobs = (int)(1 / resourceFraction);
		int numWorkerNodes = numNodes - numJobs;
		int numCoresPerNode = Integer.parseInt(inputsTable.get("numCoresPerNode"));
		double memoryPerWorkerNode = memoryPerNode;
		//Calculate the memory available for raw Spark
		double rawSparkMemoryPerNode = Common.calculateRawSparkMem(memoryPerWorkerNode);
		insertYarnNodeManagerMemRecommendation (Integer.toString((int)rawSparkMemoryPerNode) + "g", inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		insertYarnSchedulerMemRecommendation (Integer.toString((int)rawSparkMemoryPerNode) + "g", inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		double effectiveMemoryPerNode = rawSparkMemoryPerNode * resourceFraction;
		//Set driver memory + cores
		int driverMemoryValue = (int) (effectiveMemoryPerNode * Common.driverMemorySafetyFraction);
		Common.setDriverMemory(Integer.toString(driverMemoryValue) , inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnDriverMemoryOverhead(driverMemoryValue, inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		Common.setDriverCores(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		//Calculate and Set Executor Memory + Overhead + Instances
		double idealExecutorMemoryWithOverhead = Common.idealExecutorMemory * (1 + executorMemoryOverheadFraction);
		int calculatedNumExecutorsPerNode = (int)(effectiveMemoryPerNode / idealExecutorMemoryWithOverhead);
		Common.calculateNumExecsAndMem(calculatedNumExecutorsPerNode, effectiveMemoryPerNode, idealExecutorMemoryWithOverhead);
		double executorMemoryWithoutOverhead = Common.executorMemoryValue / (1 + executorMemoryOverheadFraction);
		Common.setExecutorMemory(Integer.toString((int)executorMemoryWithoutOverhead), optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnExecutorMemoryOverhead (executorMemoryWithoutOverhead, optionsTable, recommendationsTable, commandLineParamsTable);
		//Calculate and set executor cores
		Common.setExecutorInstances (Integer.toString(Common.numExecutorsPerNode * numWorkerNodes),  optionsTable, recommendationsTable, commandLineParamsTable);
		int effectiveCoresPerNode = (int) (resourceFraction * numCoresPerNode);
		int coresPerExecutor =  (int) (effectiveCoresPerNode / Common.numExecutorsPerNode);
		Common.setExecutorCores(Integer.toString(coresPerExecutor), inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	private static void insertYarnNodeManagerMemRecommendation (String recommendation, Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		recommendationsTable.put("CM-environment - yarn.nodemanager.resource.memory-mb", "Yarn Mode Only: Recommended to set YARN Container: " + recommendation);
	}
	
	private static void insertYarnSchedulerMemRecommendation (String recommendation, Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		recommendationsTable.put("CM-environment - yarn.scheduler.maximum-allocation-mb", "Yarn Mode Only: Recommended to set YARN Container: " + recommendation);
	}
	
	private static void insertYarnNodeManagerCoresRecommendation (Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		String recommendation = inputsTable.get("numCoresPerNode");
		recommendationsTable.put("CM-environment - yarn.nodemanager.resource.cpu-vcores", "Yarn Mode Only: Number of virtual CPU cores that can be allocated for containers: " + recommendation);
	}
	
	private static void insertYarnSchedulerCoresRecommendation (Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		String recommendation = inputsTable.get("numCoresPerNode");
		recommendationsTable.put("CM-environment - yarn.scheduler.maximum-allocation-vcores", "Yarn Mode Only: The largest number of virtual CPU cores that can be requested for a container: " + recommendation);
	}

	public static void setYarnExecutorMemoryOverhead(double executorMemory, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		double calculatedYarnExecutorMemOverhead = executorMemory * executorMemoryOverheadFraction;
		yarnExecutorMemoryOverhead = Integer.toString((int)(calculatedYarnExecutorMemOverhead * 1000));
		optionsTable.put("spark.yarn.executor.memoryOverhead", yarnExecutorMemoryOverhead);
		recommendationsTable.put("spark.yarn.executor.memoryOverhead", "Increase this if YARN containers fail/run out of memory.");
	}

	public static void setYarnDriverMemoryOverhead(int driverMemory, Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		double calculatedYarnDriverMemOverhead = driverMemory * driverMemoryOverheadFraction;
		yarnDriverMemoryOverhead = Integer.toString((int)(calculatedYarnDriverMemOverhead * 1000));
		optionsTable.put("spark.yarn.driver.memoryOverhead", yarnDriverMemoryOverhead);
		recommendationsTable.put("spark.yarn.driver.memoryOverhead", "Increase this if YARN containers fail/run out of memory.");
	}

	private static void setSchedulerMinRegisteredResourcesRatio(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.scheduler.minRegisteredResourcesRatio", schedulerMinRegisteredResourcesRatio);
	}
}
