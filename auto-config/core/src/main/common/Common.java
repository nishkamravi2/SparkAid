package core.src.main.common;
import java.util.*;

public class Common {
	
		//Heuristic Configured Parameters
		static String driverMemory = ""; 
		static String executorMemory = "";
		static String driverCores = "";
		static String schedulerMode = "";
		static String coresMax = ""; 
		static String shuffleConsolidateFiles = "";
		static String defaultParallelism = "";
		static String rddCompress = "false";
		static String storageLevel = "";
		public static String executorCores = ""; 
		public static String executorInstances = ""; 
		
		//Default Configuration Values
		//Application Properties
		static String driverMaxResultSize = "0";
	
		//RunTime
		static String driverExtraJavaOptions = "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Dsun.io.serialization.extendedDebugInfo=true";
		static String executorExtraJavaOptions = "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Dsun.io.serialization.extendedDebugInfo=true";
		
		//Compression and Serialization
		static String serializer = "org.apache.spark.serializer.JavaSerializer";

		//Execution Behaviour
		static String storageMemoryFraction = "0.6"; 
		static String storageSafetyFraction = "0.9";
		static String storageUnrollFraction = "0.2"; //fraction of storageMemoryFraction
		
		//External variables used for configurations
		public static double idealExecutorMemory = 8; //gb
		public static double executorMemoryValue = idealExecutorMemory;  
		public static int numExecutorsPerNode = 0;
		
		//System Overhead, OS + Resource Manager (e.g. CM) + other processes running in background
		public static double systemOverheadBuffer = 1.00;
		public static double systemOverheadBufferTier1 = 0.850;
		public static double systemOverheadBufferTier2 = 0.900;
		public static double systemOverheadBufferTier3 = 0.950;

		//Spark Overhead Buffer
		public static double sparkOverheadBuffer = 1.00;
		public static double sparkOverheadBufferTier1 = 0.850;
		public static double sparkOverheadBufferTier2 = 0.900;
		public static double sparkOverheadBufferTier3 = 0.950;
		
		public static double driverMemorySafetyFraction = 0.80;
		public static double executorUpperBoundLimitG = 64; //gb
		static double unserializedFactor = 4; //experimentally determined at ~3
		static double serializedUncompressedFactor = 0.8; //experimentally determined at ~0.5
		static double serializedCompressedFactor = serializedUncompressedFactor * 0.8;
		
		static double daemonMemoryFraction = 0.15;
		static double daemonMemoryMin = 0.5; //gb
		static double workerMaxHeapsize = 4;
		
		static int numExecutorsPerNodeUpperBound = 4;
		static int numExecutorsPerNodeLowerBound = 2;
		
		static double parallelismFactor = 4;
		
		public static void configureStandardSettings(
				Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable,
				Hashtable<String, String> recommendationsTable,
				Hashtable<String, String> commandLineParamsTable) {

			// Set Application Properties
			setApplicationProperties(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			// Set Shuffle Behavior
			setShuffleBehavior(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			// Set Compression and Serialization
			setCompressionSerialization(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			// Set Execution Behavior
			setExecutionBehavior(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			// Set Networking
			setNetworking(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			// Set Spread Deploy Mode
			setDeploySpreadOut(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			//Set Environment Variable Recommendations
			setEnvironment(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			//Set Java Options
			setExecutorExtraJavaOptions(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			setDriverExtraJavaOptions(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}
		
		public static void setApplicationProperties(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			setExecMemCoresInstances(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable); //this must come after setDriverMemory
		    setCoresMax(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setMaxResultSize(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}
		
		public static void setShuffleBehavior(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		    setConsolidateFiles(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleManager(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleMemoryFraction(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setStorageSafetyFraction(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}

		public static void setCompressionSerialization(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		    setKryoClassesToRegister(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setKyroserializerBufferMax(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSerializer(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setStorageLevel(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}
		
		public static void setExecutionBehavior(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		    setDefaultParallelism(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}
		
		public static void setNetworking(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		    setAkkaTimeout(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setLocalityWait(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSchedulerMode(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSpeculation(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}
		
		public static void setEnvironment(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			setLocalDirEnv(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setWorkerDirEnv(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}
		
		private static void setExecMemCoresInstances(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
			double memoryPerNode = Double.parseDouble(inputsTable.get("memoryPerNode")); //in mb
			int numNodes = Integer.parseInt(inputsTable.get("numNodes"));
			int numJobs = (int)(1 / resourceFraction);
			int numWorkerNodes = numNodes - numJobs;
			int numCoresPerNode = Integer.parseInt(inputsTable.get("numCoresPerNode"));
			double memoryPerWorkerNode = memoryPerNode;
			//Calculate the memory available for raw Spark
			double rawSparkMemoryPerNode = calculateRawSparkMem(memoryPerWorkerNode);
			setDaemonMaxHeapSizeRecommendations(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable, rawSparkMemoryPerNode);
			double effectiveMemoryPerNode = resourceFraction * rawSparkMemoryPerNode;
			//Calculate and set driver memory + cores
			setDriverMemory(Integer.toString((int)(effectiveMemoryPerNode * driverMemorySafetyFraction)) , inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			setDriverCores(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			//Calculate and set executor memory + instances
			int calculatedNumExecutorsPerNode = (int)(effectiveMemoryPerNode / idealExecutorMemory);
			calculateNumExecsAndMem(calculatedNumExecutorsPerNode, effectiveMemoryPerNode, idealExecutorMemory);
			setExecutorMemory(Integer.toString((int)executorMemoryValue), optionsTable, recommendationsTable, commandLineParamsTable);
			setPythonWorkerMemory(Integer.toString((int)executorMemoryValue), optionsTable, recommendationsTable, commandLineParamsTable);
			setExecutorInstances (Integer.toString(numExecutorsPerNode * numWorkerNodes),  optionsTable, recommendationsTable, commandLineParamsTable);
			//Calculate and set executor cores
			int effectiveCoresPerNode = (int) (resourceFraction * numCoresPerNode);
			int coresPerExecutor =  (int) (effectiveCoresPerNode / numExecutorsPerNode);
			setExecutorCores(Integer.toString(coresPerExecutor), inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);	
		}
		
		public static double calculateRawSparkMem(double memoryPerWorkerNode) {
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
			return memoryPerWorkerNode * systemOverheadBuffer * sparkOverheadBuffer;
		}
		
		public static void calculateNumExecsAndMem(int calculatedNumExecutorsPerNode, double effectiveMemoryPerNode, double idealExecutorMemory){
			executorMemoryValue = idealExecutorMemory;
			boolean recalculateFlag = false;
			
			if (calculatedNumExecutorsPerNode > numExecutorsPerNodeUpperBound){
				numExecutorsPerNode = numExecutorsPerNodeUpperBound;
				recalculateFlag = true;
			}
			else if (calculatedNumExecutorsPerNode < numExecutorsPerNodeLowerBound){
				numExecutorsPerNode = numExecutorsPerNodeLowerBound;
				recalculateFlag = true;
			}
			else{ 
				numExecutorsPerNode = calculatedNumExecutorsPerNode;
				double currMemSizePerNode = idealExecutorMemory * numExecutorsPerNode;
				double leftOverMemPerNode = effectiveMemoryPerNode - currMemSizePerNode;
				if(leftOverMemPerNode > (idealExecutorMemory / 2)){
					recalculateFlag = true;
				}
			}
			
			if(recalculateFlag){
				executorMemoryValue = effectiveMemoryPerNode/numExecutorsPerNode;
			}
			executorMemoryValue = Math.min(executorUpperBoundLimitG, executorMemoryValue);
		}

		private static void setDaemonMaxHeapSizeRecommendations(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, 
				Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable, double rawSparkMemoryPerNode) {
			double daemonMemoryPerNode = rawSparkMemoryPerNode * daemonMemoryFraction;
			daemonMemoryPerNode = Math.max(daemonMemoryPerNode, daemonMemoryMin);
			insertDaemonMasterMaxHeapSizeRecommendation(Integer.toString((int)(daemonMemoryPerNode)) + "g", inputsTable, optionsTable, recommendationsTable, commandLineParamsTable); 
			insertExecutorTotalMaxHeapsizeRecommendation(Integer.toString((int)daemonMemoryPerNode) + "g", inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			insertDaemonWorkerMaxHeapSizeRecommendation(Integer.toString((int)rawSparkMemoryPerNode) + "g", inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}
		
		public static void setExecutorInstances (String value, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			executorInstances = value;
			optionsTable.put("spark.executor.instances", executorInstances);
		}
		
		public static void setDriverCores(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			driverCores = inputsTable.get("numCoresPerNode");
			optionsTable.put("spark.driver.cores", driverCores);
			commandLineParamsTable.put("--driver-cores", driverCores);
		}
		
		private static void setMaxResultSize(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.maxResultSize", driverMaxResultSize);
		}
	
		public static void setDriverMemory(String value, Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			driverMemory = value + "g";
			optionsTable.put("spark.driver.memory", driverMemory);
			commandLineParamsTable.put("--driver-memory", driverMemory);
		}
	
		public static void setExecutorMemory (String value, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			executorMemory = value + "g";
			optionsTable.put("spark.executor.memory", executorMemory);
		}
	
		private static void setLocalDirEnv(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			recommendationsTable.put("environment - SPARK_LOCAL_DIRS", "Ensure this location has TB's of space");
		}
		
		private static void setWorkerDirEnv(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			recommendationsTable.put("environment - SPARK_WORKER_DIR", "Ensure this location has TB's of space");
		}
	
		private static void setDriverExtraJavaOptions(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			double memoryPerNode = Double.parseDouble(inputsTable.get("memoryPerNode"));
			if (memoryPerNode < 32){
				driverExtraJavaOptions += " -XX:+UseCompressedOops";
			}
			optionsTable.put("spark.driver.extraJavaOptions", driverExtraJavaOptions);
		}
	
		private static void setExecutorExtraJavaOptions(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			double memoryPerNode = Double.parseDouble(inputsTable.get("memoryPerNode"));
			if (memoryPerNode < 32){
				executorExtraJavaOptions += " -XX:+UseCompressedOops";
			}
			optionsTable.put("spark.executor.extraJavaOptions", executorExtraJavaOptions);
		}
		
		private static void setPythonWorkerMemory (String value, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.python.worker.memory", value);
		}
		
		//Shuffle Behavior	
		private static void setConsolidateFiles(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			if (inputsTable.get("fileSystem").equals("ext4") || inputsTable.get("fileSystem").equals("xfs")){
				shuffleConsolidateFiles = "true";
			}
			optionsTable.put("spark.shuffle.consolidateFiles", shuffleConsolidateFiles);
		}
	
		private static void setShuffleManager(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			recommendationsTable.put("spark.shuffle.manager", "In some cases, might want to try Hash instead of default Sort based shuffle for better performance.");
		}
	
		private static void setShuffleMemoryFraction(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			recommendationsTable.put("spark.shuffle.memoryFraction", "Increase this to a max of 0.8 if there is no RDD caching/persistence in the app");
		}
		
		private static void setStorageSafetyFraction(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.storage.safetyFraction", storageSafetyFraction);
		}
		
		private static void setKryoClassesToRegister(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			recommendationsTable.put("spark.kryo.classesToRegister", "If you use Kryo serialization, specify list of custom class names to register with Kryo.");
		}
	
		private static void setKyroserializerBufferMax(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			recommendationsTable.put("spark.kryoserializer.buffer.max", "Increase this if you get a \"buffer limit exceeded\" exception (set size to largest object to be serialized).");
		}
	
		private static void setRddCompress(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.rdd.compress", rddCompress);
		}
	
		private static void setSerializer(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			serializer = "org.apache.spark.serializer.KryoSerializer";
			optionsTable.put("spark.serializer", serializer);
		}

		private static void setStorageLevel(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			
			double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
			int numJobs = (int)(1/resourceFraction);
			double numNodes = Double.parseDouble(inputsTable.get("numNodes"));
			double memoryPerNode = Double.parseDouble(inputsTable.get("memoryPerNode"));
			double inputDataSize = Double.parseDouble(inputsTable.get("inputDataSize"));
			double storageSafetyFractionValue = Double.parseDouble((storageSafetyFraction));
			double storageMemoryFractionValue = Double.parseDouble((storageMemoryFraction));
			double storageMemoryAvailableFraction = 1 - Double.parseDouble(storageUnrollFraction);
			
			double availableMemory = resourceFraction * (numNodes - numJobs) * memoryPerNode * storageSafetyFractionValue * storageMemoryFractionValue * storageMemoryAvailableFraction;
			
			double inputUnserialized = inputDataSize * unserializedFactor;
			double inputUncompressedSerialized = inputDataSize * serializedUncompressedFactor;
			double inputCompressedSerialized = inputDataSize * serializedCompressedFactor;
			
			if (inputUnserialized < availableMemory){
				storageLevel = "MEMORY_ONLY";
			}else if (inputUncompressedSerialized < availableMemory){
				storageLevel = "MEMORY_ONLY_SER";
			}else if (inputCompressedSerialized < availableMemory){
				storageLevel = "MEMORY_ONLY_SER";
				rddCompress = "true";
				setRddCompress(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			}else{
				storageLevel = "MEMORY_AND_DISK_SER";
			}
			optionsTable.put("spark.storage.level", storageLevel);	
		}
		
		//Execution Behavior
		public static void setExecutorCores(String value, Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			executorCores = value;
			optionsTable.put("spark.executor.cores", executorCores);
		}

		private static void setDefaultParallelism(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
			int numJobs = (int)(1/resourceFraction);
			int numNodes = Integer.parseInt(inputsTable.get("numNodes"));
			int numCoresPerNode = Integer.parseInt(inputsTable.get("numCoresPerNode"));
			int totalCoresAvailable = (int)(numCoresPerNode * (numNodes - numJobs) * resourceFraction);
			int calculatedParallelism = (int)(parallelismFactor * totalCoresAvailable);
			defaultParallelism = Integer.toString(calculatedParallelism);
			optionsTable.put("spark.default.parallelism", defaultParallelism);
			recommendationsTable.put("spark.default.parallelism","Try doubling this value for potential performance gains. Set the same value in the code. "
					+ "E.g. sc.textFile( path/to/file, default-parallelism-value)");
		}
		
		//Networking
		private static void setAkkaTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			recommendationsTable.put("spark.akka.timeout", "Increase if GC pauses cause problem");
		}
		
		public static void setCoresMax(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			int allocateCoresMax = Integer.parseInt(executorCores) * Integer.parseInt(executorInstances);
			coresMax = Integer.toString(allocateCoresMax);
			optionsTable.put("spark.cores.max", coresMax);
		}
	
		private static void setLocalityWait(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			recommendationsTable.put("spark.locality.wait", "Increase this value if long GC pauses");
		}
	
		private static void setSchedulerMode(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			if (Double.parseDouble(inputsTable.get("resourceFraction")) < 1.0){
				schedulerMode = "FAIR";
			}
			else{
				schedulerMode = "FIFO";
			}
			optionsTable.put("spark.scheduler.mode", schedulerMode);
		}
	
		private static void setSpeculation(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			recommendationsTable.put("spark.speculation", "Set to true if stragglers are found");
		}
		
		private static void setDeploySpreadOut(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			//Whether the standalone cluster manager should spread applications out across nodes or try to consolidate them onto as few nodes as possible. 
			recommendationsTable.put("spark.deploy.spreadOut", "Spreading out is usually better for data locality in HDFS");
		}
		
		private static void insertExecutorTotalMaxHeapsizeRecommendation (String recommendation, Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			recommendationsTable.put("CM-environment - SPARK_WORKER_MEMORY (executor_total_max_heapsize)","Standalone Mode only: Total amount of memory to allow Spark applications to use on the machine: " + recommendation);
		}
		private static void insertDaemonMasterMaxHeapSizeRecommendation (String recommendation, Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			recommendationsTable.put("CM-environment - SPARK_DAEMON_MEMORY (master_max_heapsize)","Standalone Mode only: Total amount of memory to allow Spark applications to use on the machine: " + recommendation);
		}
		private static void insertDaemonWorkerMaxHeapSizeRecommendation (String recommendation, Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			recommendationsTable.put("CM-environment - SPARK_DAEMON_MEMORY (worker_max_heapsize) ","Standalone Mode only: Memory to allocate to the Spark master and worker daemons themselves: " + recommendation);
		}
		
	}
