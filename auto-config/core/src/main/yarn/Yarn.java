package core.src.main.yarn;
import java.util.ArrayList;
import java.util.Hashtable;

public class Yarn {
	
	static String yarnAMMemory = ""; //512m
	//YARN AM in Cluster Mode	
	static String driverCores = ""; //1  
	//YARN AM in Client Mode	
	static String yarnAMCores = ""; //1
	static String yarnAMWaitTime = ""; //100s
	static String yarnSubmitFileReplication = ""; //3
	static String yarnPreserveStagingFiles = ""; //false
	static String yarnSchedulerHeartbeatIntervalms = ""; //5000
	static String yarnMaxExecutorFailures = ""; //numExecutors * 2, with min of 3
	static String yarnHistoryServerAddress = ""; //none
	static String yarnDistArchives = ""; //none
	static String yarnDistFiles = ""; //none
	static String executorInstances = ""; //2
	static String yarnExecutorMemoryOverhead = ""; //executorMemory * 0.10, with min of 384
	static String yarnDriverMemoryOverhead = ""; //driverMemory * 0.07, with min of 384
	static String yarnAMMemoryOverhead = ""; //AMMemory * 0.07, with min of 384
	static String yarnAMPort = ""; //random
	static String yarnQueue = ""; //default
	static String yarnJar = ""; //none
	static String yarnAccessNameNodes = ""; //none
	//array to set all the different AM Env variables
	static ArrayList<String> yarnAppMasterEnvVariablesArray = new ArrayList<String>();
	//array to set all the different AM Env values for corresponding variables
	static ArrayList<String> yarnAppMasterEnvValuesArray = new ArrayList<String>();
	static String yarnContainerLauncherMaxThreads = ""; //25
	static String yarnAMExtraJavaOptions = ""; //none
	static String yarnAMExtraLibraryPath = ""; //none
	static String yarnMaxAppAttempts = ""; //yarn.resourcemanager.am.max-attempts in YARN
	static String yarnSubmitWaitAppCompletion = ""; //true
	
	//these variables are in tuning guide 2014, but not yarn configs
	//yarn.nodemanager.resource.memory-mb controls the maximum sum of memory used by the containers on each node.
	//yarn.nodemanager.resource.cpu-vcores controls the maximum sum of cores used by the containers on each node.
	
	public static void configureYarnSettings(
			Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable,
			Hashtable<String, String> recommendationsTable,
			Hashtable<String, String> commandLineParamsTable) {
		setYarn(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	public static void setYarn(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		setYarnAMMemory(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setDriverCores(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnAMCores(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnAMWaitTime(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnSubmitFileReplication(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnPreserveStagingFiles(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnSchedulerHeartbeatIntervalms(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnMaxExecutorFailures(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnHistoryServerAddress(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnDistArchives(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnDistFiles(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setExecutorInstances(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnExecutorMemoryOverhead(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnDriverMemoryOverhead(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnAMMemoryOverhead(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnAMPort(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnQueue(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnJar(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnAccessNameNodes(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnAppMasterEnvironmentVariableName(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnContainerLauncherMaxThreads(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnAMExtraJavaOptions(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnAMExtraLibraryPath(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnMaxAppAttempts(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setYarnSubmitWaitAppCompletion(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	//Only modify this for Client mode, in cluster mode, use spark.driver.memory instead.
	public static void setYarnAMMemory(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){	
		//If Deploy Mode == Client
		if (inputsTable.get("deployMode").equals("client")){
			optionsTable.put("spark.yarn.am.memory", yarnAMMemory);
			recommendationsTable.put("spark.yarn.am.memory", "");
		}
	}
	
	//YARN AM Cluster mode
	//This will override the Standalone Driver Core settings if need be.
	public static void setDriverCores(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		//If Deploy Mode == Cluster
		if (inputsTable.get("deployMode").equals("cluster")){
			optionsTable.put("spark.driver.cores", driverCores);
			recommendationsTable.put("spark.driver.cores", "");
		}
	}
	
	//YARN AM Client mode
	public static void setYarnAMCores(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		//If Deploy Mode == Client
		if (inputsTable.get("deployMode").equals("client")){
			optionsTable.put("spark.yarn.am.cores", yarnAMCores);
			recommendationsTable.put("spark.yarn.am.cores", "");
		}
	}

	public static void setYarnAMWaitTime(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.am.waitTime", yarnAMWaitTime);
		recommendationsTable.put("spark.yarn.am.waitTime", "");
	}

	public static void setYarnSubmitFileReplication(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.submit.file.replication", yarnSubmitFileReplication);
		recommendationsTable.put("spark.yarn.submit.file.replication", "");
	}

	public static void setYarnPreserveStagingFiles(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.preserve.staging.files", yarnPreserveStagingFiles);
		recommendationsTable.put("spark.yarn.preserve.staging.files", "");
	}

	public static void setYarnSchedulerHeartbeatIntervalms(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.scheduler.heartbeat.interval-ms", yarnSchedulerHeartbeatIntervalms);
		recommendationsTable.put("spark.yarn.scheduler.heartbeat.interval-ms", "");
	}

	public static void setYarnMaxExecutorFailures(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.max.executor.failures", yarnMaxExecutorFailures);
		recommendationsTable.put("spark.yarn.max.executor.failures", "");
	}

	public static void setYarnHistoryServerAddress(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.historyServer.address", yarnHistoryServerAddress);
		recommendationsTable.put("spark.yarn.historyServer.address", "");
	}

	public static void setYarnDistArchives(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.dist.archives", yarnDistArchives);
		recommendationsTable.put("spark.yarn.dist.archives", "");
	}

	public static void setYarnDistFiles(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.dist.files", yarnDistFiles);
		recommendationsTable.put("spark.yarn.dist.files", "");
	}

	public static void setExecutorInstances(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.executor.instances", executorInstances);
		recommendationsTable.put("spark.executor.instances", "");
	}

	public static void setYarnExecutorMemoryOverhead(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.executor.memoryOverhead", yarnExecutorMemoryOverhead);
		recommendationsTable.put("spark.yarn.executor.memoryOverhead", "");
	}

	public static void setYarnDriverMemoryOverhead(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.driver.memoryOverhead", yarnDriverMemoryOverhead);
		recommendationsTable.put("spark.yarn.driver.memoryOverhead", "");
	}

	//Same as spark.yarn.driver.memoryOverhead, but for the Application Master in client mode.
	public static void setYarnAMMemoryOverhead(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		//If Deploy Mode == Client
		if (inputsTable.get("deployMode").equals("client")){
			optionsTable.put("spark.yarn.am.memoryOverhead", yarnAMMemoryOverhead);
			recommendationsTable.put("spark.yarn.am.memoryOverhead", "");
		}
	}

	// In YARN client mode, this is used to communicate between the Spark driver running on a gateway and the Application Master running on YARN. In YARN cluster mode, this is used for the dynamic executor feature, where it handles the kill from the scheduler backend.
	public static void setYarnAMPort(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.am.port", yarnAMPort);
		recommendationsTable.put("spark.yarn.am.port", "");
	}

	public static void setYarnQueue(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.queue", yarnQueue);
		recommendationsTable.put("spark.yarn.queue", "");
	}

	public static void setYarnJar(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.jar", yarnJar);
		recommendationsTable.put("spark.yarn.jar", "");
	}

	public static void setYarnAccessNameNodes(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.access.namenodes", yarnAccessNameNodes);
		recommendationsTable.put("spark.yarn.access.namenodes", "");
	}

	//In yarn-cluster mode this controls the environment of the SPARK driver and in yarn-client mode it only controls the environment of the executor launcher.
	public static void setYarnAppMasterEnvironmentVariableName(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		for (int i = 0; i < yarnAppMasterEnvVariablesArray.size(); i++){
			String optionVariable = "spark.yarn.appMasterEnv." + yarnAppMasterEnvVariablesArray.get(i);
			optionsTable.put(yarnAppMasterEnvValuesArray.get(i), optionVariable);
			recommendationsTable.put(optionVariable, "");
		}
	}

	public static void setYarnContainerLauncherMaxThreads(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.containerLauncherMaxThreads", yarnContainerLauncherMaxThreads);
		recommendationsTable.put("spark.yarn.containerLauncherMaxThreads", "");
	}

	public static void setYarnAMExtraJavaOptions(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		//In cluster mode, use spark.driver.extraJavaOptions instead.
		//If Deploy Mode == Client
		if (inputsTable.get("deployMode").equals("client")){
			optionsTable.put("spark.yarn.am.extraJavaOptions", yarnAMExtraJavaOptions);
			recommendationsTable.put("spark.yarn.am.extraJavaOptions", "");
		}
	}

	public static void setYarnAMExtraLibraryPath(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		//If Deploy Mode == Client
		if (inputsTable.get("deployMode").equals("client")){		
		optionsTable.put("spark.yarn.am.extraLibraryPath", yarnAMExtraLibraryPath);
		recommendationsTable.put("spark.yarn.am.extraLibraryPath", "");
		}
	}

	public static void setYarnMaxAppAttempts(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.maxAppAttempts", yarnMaxAppAttempts);
		recommendationsTable.put("spark.yarn.maxAppAttempts", "");
	}

	public static void setYarnSubmitWaitAppCompletion(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.yarn.submit.waitAppCompletion", yarnSubmitWaitAppCompletion);
		recommendationsTable.put("spark.yarn.submit.waitAppCompletion", "");
	}
}