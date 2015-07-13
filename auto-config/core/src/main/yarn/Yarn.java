package core.src.main.yarn;
import java.util.ArrayList;
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
	
	//
	static String executorCores = "4";
	
	//Variables for default Overhead Memory Setting, this is an inferred setting
	static double executorMemoryOverheadFraction = 0.10; //recommended by config guide
	static double driverMemoryOverheadFraction = 0.07; //recommended by config guide
	static double AMMemoryOverheadFraction = 0.07; //recommended by config guide
	
	//External variables not in Spark but used for configurations
	static double executorRoundingBuffer = 0.98;
	static double driverMemorySafetyFraction = 0.8;
	
	//YARN AM Defaults
	static String yarnAMWaitTime = "100000"; //100000ms
	static String yarnSubmitFileReplication = "3"; //3
	static String yarnPreserveStagingFiles = "false"; 
	static String yarnSchedulerHeartbeatIntervalms = "5000"; //5000ms
	static String yarnMaxExecutorFailures = ""; //numExecutors * 2, with min of 3
	static String yarnHistoryServerAddress = ""; //none
	static String yarnDistArchives = ""; //none
	static String yarnDistFiles = ""; //none
	static String yarnAMPort = ""; //random
	static String yarnQueue = ""; //default
	static String yarnJar = ""; //none
	static String yarnAccessNameNodes = ""; //none
	static ArrayList<String> yarnAppMasterEnvVariablesArray = new ArrayList<String>(); //array to set all the different AM Env variables
	static ArrayList<String> yarnAppMasterEnvValuesArray = new ArrayList<String>(); //array to set all the different AM Env values for corresponding variables
	static String yarnContainerLauncherMaxThreads = "25";//25
	static String yarnAMExtraJavaOptions = ""; //none
	static String yarnAMExtraLibraryPath = ""; //none
	static String yarnMaxAppAttempts = ""; //yarn.resourcemanager.am.max-attempts in YARN
	static String yarnSubmitWaitAppCompletion = "true";//true
	
	
	
	public static void configureYarnSettings( Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		
		setExecutorCores(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnDefaults(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setExecMemCoresInstances(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		
		//Client only settings
		if (inputsTable.get("deployMode").equals("client")){
			setYarnAMMemory(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			setYarnAMMemoryOverhead(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			setYarnAMCores(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			setYarnAMExtraJavaOptions(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			setYarnAMExtraLibraryPath(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}else if (inputsTable.get("deployMode").equals("cluster")){
			setYarnDriverMemoryOverhead(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			setDriverMemory(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable); //same as in standalone for now, will override standalone if different.
			setDriverCores(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable); //same as in standalone for now, will override standalone if different.
		}else{
			//wrong deployMode input
		}
			
	}
	
	public static void setYarnDefaults(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		
		setYarnAMWaitTime(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnSubmitFileReplication(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnPreserveStagingFiles(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnSchedulerHeartbeatIntervalms(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnMaxExecutorFailures(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnHistoryServerAddress(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnDistArchives(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnDistFiles(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setExecutorInstances(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnAMPort(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnQueue(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnJar(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnAccessNameNodes(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnAppMasterEnvironmentVariableName(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnContainerLauncherMaxThreads(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnMaxAppAttempts(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnSubmitWaitAppCompletion(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		
		
		setSchedulerMinRegisteredResourcesRatio(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	private static void setExecMemCoresInstances(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){

		//for now assume container memory = nodeMemory for YARN
		double memoryPerNode = UtilsConversion.parseMemory(inputsTable.get("memoryPerNode")); //in mb
		int numNodes = Integer.parseInt(inputsTable.get("numNodes"));
		int numWorkerNodes = numNodes - 1;
		int numCoresPerNode = Integer.parseInt(inputsTable.get("numCoresPerNode"));
		
		//add in heuristics inputDataSize soon
		int inputDataSize = Integer.parseInt(inputsTable.get("inputDataSize"));
		
		double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
		double effectiveMemoryPerNode = resourceFraction * memoryPerNode;
		int effectiveCoresPerNode = (int) (resourceFraction * numCoresPerNode);
		
		//for now we decide to assign 4 cores per executor.
		int desiredCoresPerExecutor = Integer.parseInt(executorCores); //for now 16, 8 and 4 executors are recommended. They run on average at the same time for pagerank
		int targetExecutorNumPerNode = effectiveCoresPerNode / desiredCoresPerExecutor;
		double totalMemoryPerExecutor = effectiveMemoryPerNode / targetExecutorNumPerNode * executorRoundingBuffer; //0.98 as a safety buffer for rounding/conversions
		
		//assuming a default of 0.10 overhead per executor, calculate and set executor memory. this will override standalone setting
		double executorPerMemory = totalMemoryPerExecutor / (1+executorMemoryOverheadFraction);
		setExecutorMemory(Integer.toString((int)executorPerMemory) + "g", "", optionsTable, recommendationsTable, commandLineParamsTable);
		
		//calculate and set executor overhead
		double yarnExecutorOverhead = totalMemoryPerExecutor / (1+executorMemoryOverheadFraction) * executorMemoryOverheadFraction *1000; //convert back to mb
		yarnExecutorMemoryOverhead = String.valueOf((int)(yarnExecutorOverhead));
		setYarnExecutorMemoryOverhead (yarnExecutorMemoryOverhead, "",  optionsTable, recommendationsTable, commandLineParamsTable);
		
		//set executor.cores
//		setExecutorCores (Integer.toString(desiredCoresPerExecutor), "",  optionsTable, recommendationsTable, commandLineParamsTable);
		
		//set executor.instances
		int totalExecutorInstances =  targetExecutorNumPerNode * numWorkerNodes;
		setExecutorInstances (Integer.toString(totalExecutorInstances), "",  optionsTable, recommendationsTable, commandLineParamsTable);
		
	}
	
	private static void setExecutorInstances (String value, String recommendation, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		
		optionsTable.put("spark.executor.instances", value);
		if (recommendation.length() > 0)
			recommendationsTable.put("spark.executor.instances", recommendation);
	}
	
	public static void setExecutorCores(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.executor.cores", executorCores);
	}

//	private static void setExecutorCores (String value, String recommendation, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
//		
//		optionsTable.put("spark.executor.cores", value);
//		if (recommendation.length() > 0)
//			recommendationsTable.put("spark.executor.cores", recommendation);
//	}
	
	private static void setExecutorMemory (String value, String recommendation, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		
		optionsTable.put("spark.executor.memory", value);
		if (recommendation.length() > 0)
			recommendationsTable.put("spark.executor.memory", recommendation);
		
	}
	
	//Only modify this for Client mode, in cluster mode, use spark.driver.memory instead.
	public static void setYarnAMMemory(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){	

		double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
		double memoryPerNode = UtilsConversion.parseMemory(inputsTable.get("memoryPerNode")); //in mb
		//Set driver memory 0.8 of current node's memory - driverMemorySafetyFraction
		double targetAMMemory = memoryPerNode * driverMemorySafetyFraction * resourceFraction;
		double allocatedAMMemory = targetAMMemory / (1 + AMMemoryOverheadFraction);
		yarnAMMemory = Integer.toString((int)allocatedAMMemory) + "g";
		optionsTable.put("spark.yarn.am.memory", yarnAMMemory);
		//remove command line params
		commandLineParamsTable.remove("--driver-memory"); 
		//remove redundant settings for client mode
		optionsTable.remove("spark.driver.memory");
	}
	
	//YARN AM Cluster mode
	//This will override the Standalone Driver Core settings if need be. not used for now.
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
		double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
		double memoryPerNode = UtilsConversion.parseMemory(inputsTable.get("memoryPerNode")); //in mb
		//Set driver memory 0.8 of current node's memory - driverMemorySafetyFraction 
		double targetDriverMemory = memoryPerNode * driverMemorySafetyFraction * resourceFraction;
		
		double allocatedDriverMemory = targetDriverMemory / (1.0 + driverMemoryOverheadFraction);
		
		String driverMemory = Integer.toString((int)allocatedDriverMemory) + "g";
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
		//remove command line param
		commandLineParamsTable.remove("--driver-cores"); 
		//remove redundant settings for client mode
		optionsTable.remove("spark.driver.cores");
	}

	public static void setYarnAMWaitTime(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.am.waitTime", yarnAMWaitTime);
	}

	public static void setYarnSubmitFileReplication(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.submit.file.replication", yarnSubmitFileReplication);
	}

	public static void setYarnPreserveStagingFiles(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.preserve.staging.files", yarnPreserveStagingFiles);
	}

	public static void setYarnSchedulerHeartbeatIntervalms(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.scheduler.heartbeat.interval-ms", yarnSchedulerHeartbeatIntervalms);
	}

	public static void setYarnMaxExecutorFailures(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.max.executor.failures", yarnMaxExecutorFailures);
	}

	public static void setYarnHistoryServerAddress(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.historyServer.address", yarnHistoryServerAddress);
	}

	public static void setYarnDistArchives(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.dist.archives", yarnDistArchives);
	}

	public static void setYarnDistFiles(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.dist.files", yarnDistFiles);
	}

	public static void setExecutorInstances(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.executor.instances", executorInstances);
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
		//Set driver memory 0.8 of current node's memory - driverMemorySafetyFraction
		double targetDriverMemory = memoryPerNode * driverMemorySafetyFraction * resourceFraction / (1 + driverMemoryOverheadFraction);
		double calculatedYarnDriverMemOverhead = targetDriverMemory * driverMemoryOverheadFraction;
		yarnDriverMemoryOverhead = Integer.toString((int)calculatedYarnDriverMemOverhead * 1024);
		optionsTable.put("spark.yarn.driver.memoryOverhead", yarnDriverMemoryOverhead);
	}

	//Same as spark.yarn.driver.memoryOverhead, but for the Application Master in client mode.
	public static void setYarnAMMemoryOverhead(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		
		double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
		double memoryPerNode = UtilsConversion.parseMemory(inputsTable.get("memoryPerNode")); //in mb
		//Set driver memory 0.8 of current node's memory - driverMemorySafetyFraction
		double targetDriverMemory = memoryPerNode * driverMemorySafetyFraction * resourceFraction / (1 + AMMemoryOverheadFraction);
		double calculatedYarnAMMemOverhead = targetDriverMemory * AMMemoryOverheadFraction;
		yarnAMMemoryOverhead = Integer.toString((int)calculatedYarnAMMemOverhead * 1024);
		optionsTable.put("spark.yarn.am.memoryOverhead", yarnAMMemoryOverhead);
	}

	// In YARN client mode, this is used to communicate between the Spark driver running on a gateway and the Application Master running on YARN. In YARN cluster mode, this is used for the dynamic executor feature, where it handles the kill from the scheduler backend.
	public static void setYarnAMPort(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.am.port", yarnAMPort);
	}
	
	public static void setYarnQueue(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.queue", yarnQueue);
	}

	public static void setYarnJar(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.jar", yarnJar);
	}

	public static void setYarnAccessNameNodes(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.access.namenodes", yarnAccessNameNodes);
	}

	//In yarn-cluster mode this controls the environment of the SPARK driver and in yarn-client mode it only controls the environment of the executor launcher.
	public static void setYarnAppMasterEnvironmentVariableName(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		for (int i = 0; i < yarnAppMasterEnvVariablesArray.size(); i++){
			String optionVariable = "spark.yarn.appMasterEnv." + yarnAppMasterEnvVariablesArray.get(i);
			optionsTable.put(yarnAppMasterEnvValuesArray.get(i), optionVariable);
		}
	}

	public static void setYarnContainerLauncherMaxThreads(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.containerLauncherMaxThreads", yarnContainerLauncherMaxThreads);
	}

	//Client Mode only
	public static void setYarnAMExtraJavaOptions(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		//In cluster mode, use spark.driver.extraJavaOptions instead.
		optionsTable.put("spark.yarn.am.extraJavaOptions", yarnAMExtraJavaOptions);
	}
	//Client Mode only
	public static void setYarnAMExtraLibraryPath(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){	
		optionsTable.put("spark.yarn.am.extraLibraryPath", yarnAMExtraLibraryPath);
	}

	public static void setYarnMaxAppAttempts(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.maxAppAttempts", yarnMaxAppAttempts);
	}

	public static void setYarnSubmitWaitAppCompletion(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.submit.waitAppCompletion", yarnSubmitWaitAppCompletion);
	}
	
	//overrides standalone setting
	private static void setSchedulerMinRegisteredResourcesRatio(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.scheduler.minRegisteredResourcesRatio", "0.8");
	}
}
