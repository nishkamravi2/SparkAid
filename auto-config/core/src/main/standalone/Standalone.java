package core.src.main.standalone;
import java.util.*;

import utils.UtilsConversion;

public class Standalone {
	
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
		
		static String executorCores = ""; 
		
		//not in configuration file, does not work for 1.3.0
		static String executorInstances = ""; 
		
		//Default Values
		
		//Application Properties
		static String driverMaxResultSize = "0";
		static String localDir = "/tmp";
	
		//RunTime
		static String driverExtraJavaOptions = "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps";
		static String executorExtraJavaOptions = "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps";
		static String pythonProfileDump = ""; //none

		//Shuffle
		static String shuffleManager = "sort";
		static String shuffleMemoryFraction = "0.2"; 
		static String shuffleSafetyFraction = "0.8"; //Not in documentation
		
		//Compression and Serialization
		static String kryoClassesToRegister = ""; //insert kryo classes to register here
		static String kryoRegistrator = "";
		static String kyroserializerBufferMax = "64m";
		static String serializer = "org.apache.spark.serializer.JavaSerializer";

		//Execution Behaviour
		static String storageMemoryFraction = "0.6"; // storageMemoryFraction + storageUnrollFraction + shuffleMemoryFraction = 1
		static String storageSafetyFraction = "0.9"; //not in Spark documentation
		static String storageUnrollFraction = "0.2"; // Fraction of storageMemoryFraction
		
		//Networking
		static String akkaTimeout = "100"; //s

		//Scheduling
		static String localityWait = "3000"; //ms
		static String schedulerMinRegisteredResourcesRatio = "0.0"; //0.8 for YARN, 0.0 otherwise
		static String speculation = "false"; //false
		
		//Deploy spread mode
		static String deploySpreadOut = "true"; 
		
		//External variables not in Spark but used for configurations
		
		static double idealExecutorMemory = 8; //gb, set this as a constant at the top
		static int defaultExecutorsPerNode = 4; // set this as a constant at the top
		
		//System Overhead, OS + Resource Manager (e.g. CM) + other processes running in background
		static double systemOverheadBuffer = 1.00;
		static double systemOverheadBufferTier1 = 0.85;
		static double systemOverheadBufferTier2 = 0.90;
		static double systemOverheadBufferTier3 = 0.95;

		//Spark Overhead Buffer
		static double sparkOverheadBuffer = 1.00;
		static double sparkOverheadBufferTier1 = 0.900;
		static double sparkOverheadBufferTier2 = 0.925;
		static double sparkOverheadBufferTier3 = 0.950;
		
		static double driverMemorySafetyFraction = 0.90;
		static double executorUpperBoundLimitG = 64;
		static double unserializedFactor = 4; //experimentally determined at ~3
		static double serializedUncompressedFactor = 0.8; //experimentally determined at ~0.5
		static double serializedCompressedFactor = serializedUncompressedFactor * 0.8;
		
		///
		static double workerMaxHeapsize = 4;
		static double maxMasterDaemonFraction = 0.75;
		
		public static void configureStandardSettings(
				Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable,
				Hashtable<String, String> recommendationsTable,
				Hashtable<String, String> commandLineParamsTable) {

			// Set Application Properties
			setApplicationProperties(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			// Set RunTime Environment
			setRunTimeEnvironment(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
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
			//set executor and driver pointer flags to 32bit if JVM is < 32GB, this will reduce GC time
			setExecutorExtraJavaOptions(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			setDriverExtraJavaOptions(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);

		}
		
		public static void setApplicationProperties(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			setExecMemCoresInstances(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable); //this must come after setDriverMemory
		    setCoresMax(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			setDriverCores(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setMaxResultSize(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setLocalDir(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}
		
		public static void setRunTimeEnvironment(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		    setPythonProfileDump(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}
		
		public static void setShuffleBehavior(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		    setConsolidateFiles(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleManager(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleMemoryFraction(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleSafetyFraction(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
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
		    setStorageMemoryFraction(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setStorageUnrollFraction(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}
		
		public static void setNetworking(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		    setAkkaTimeout(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setLocalityWait(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSchedulerMinRegisteredResourcesRatio(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSchedulerMode(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSpeculation(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
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
			}
			else if (memoryPerWorkerNode <= 100){
				systemOverheadBuffer = systemOverheadBufferTier2;
			}
			else{
				systemOverheadBuffer = systemOverheadBufferTier3;
			}
			
			double rawSparkMemoryPerNode = memoryPerWorkerNode * systemOverheadBuffer;
			double rawSparkMemory = rawSparkMemoryPerNode * numWorkerNodes;
			
			insertDaemonWorkerMaxHeapSizeRecommendation(Integer.toString((int)workerMaxHeapsize) + "g", inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			insertDaemonMasterMaxHeapSizeRecommendation(Integer.toString((int)(rawSparkMemoryPerNode * maxMasterDaemonFraction)) + "g", inputsTable, optionsTable, recommendationsTable, commandLineParamsTable); //this should be less, assumption that Master has more overhead processes
			insertExecutorTotalMaxHeapsizeRecommendation(Integer.toString((int)rawSparkMemoryPerNode) + "g", inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			
			//Calculate memory available for Spark job processes
			if (rawSparkMemory <= 80){
				sparkOverheadBuffer = sparkOverheadBufferTier1;
			}
			else if (rawSparkMemory <= 160){
				sparkOverheadBuffer = sparkOverheadBufferTier2;
			}
			else{
				sparkOverheadBuffer = sparkOverheadBufferTier3;
			}
			
			double actualSparkMemory = rawSparkMemory * sparkOverheadBuffer;
			
			//User input of what fraction of resources of Spark cluster to be used
			double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
			double effectiveMemory = resourceFraction * actualSparkMemory;
			double effectiveMemoryPerNode = effectiveMemory / numNodes;
			
			setDriverMemory( Integer.toString((int)(effectiveMemoryPerNode * driverMemorySafetyFraction)) , inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			
			int numExecutorsPerNode = 0;
			double calculatedNumExecutorsPerNode = effectiveMemoryPerNode / idealExecutorMemory;
			
			
			if (calculatedNumExecutorsPerNode > 4){
				numExecutorsPerNode = 4;
			}
			else if (calculatedNumExecutorsPerNode < 2){
				numExecutorsPerNode = 2;
			}
			
			double currMemSizePerNode = idealExecutorMemory * numExecutorsPerNode;
			double leftOverMemPerNode = effectiveMemoryPerNode - currMemSizePerNode;
			
			//Calculate Memory per Executor
			double finalExecutorMemory = idealExecutorMemory;
			if (leftOverMemPerNode > (idealExecutorMemory / 2)){
				finalExecutorMemory = effectiveMemoryPerNode / defaultExecutorsPerNode;
				numExecutorsPerNode = defaultExecutorsPerNode;
			}
			
			finalExecutorMemory = Math.min(executorUpperBoundLimitG, finalExecutorMemory);
			
			//Calculate Cores Per Executor
			int effectiveCoresPerNode = (int) (resourceFraction * numCoresPerNode);
			int coresPerExecutor =  (int) (effectiveCoresPerNode / defaultExecutorsPerNode);
			executorCores = Integer.toString(coresPerExecutor);
		    setExecutorCores(executorCores, inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);	
			setExecutorMemory(Integer.toString((int)finalExecutorMemory) + "g", "", optionsTable, recommendationsTable, commandLineParamsTable);
			setPythonWorkerMemory(Integer.toString((int)finalExecutorMemory) + "g", "", optionsTable, recommendationsTable, commandLineParamsTable);
			int totalExecutorInstances =  numExecutorsPerNode * numWorkerNodes;
			setExecutorInstances (Integer.toString(totalExecutorInstances), "",  optionsTable, recommendationsTable, commandLineParamsTable);
			
		}
		
		private static void setExecutorInstances (String value, String recommendation, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			executorInstances = value;
			optionsTable.put("spark.executor.instances", executorInstances);
			if (recommendation.length() > 0)
				recommendationsTable.put("spark.executor.instances", recommendation);
		}
		
		//heuristic is now that the driver uses one whole node to itself
		private static void setDriverCores(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
			String numCoresPerNode = inputsTable.get("numCoresPerNode");
			double effectiveDriverCores = resourceFraction * Double.parseDouble(numCoresPerNode);
			driverCores = Integer.toString((int)effectiveDriverCores);
			optionsTable.put("spark.driver.cores", driverCores);
			commandLineParamsTable.put("--driver-cores", driverCores);
		}
		
		private static void setMaxResultSize(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.maxResultSize", driverMaxResultSize);
		}
	
		private static void setDriverMemory(String value, Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			driverMemory = value + "g";
			optionsTable.put("spark.driver.memory", driverMemory);
			commandLineParamsTable.put("--driver-memory", driverMemory);
		}
	
		private static void setExecutorMemory (String value, String recommendation, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.memory", value);
			if (recommendation.length() > 0)
				recommendationsTable.put("spark.executor.memory", recommendation);
		}
	
		private static void setLocalDir(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.local.dir", localDir);
			recommendationsTable.put("spark.local.dir", "Ensure this location has TB's of space");
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
	
		private static void setPythonProfileDump(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.python.profile.dump", pythonProfileDump);
			recommendationsTable.put("spark.python.profile.dump","Set directory to dump profile result before driver exiting if desired");
		}
		
		private static void setPythonWorkerMemory (String value, String recommendation, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.python.worker.memory", value);
			if (recommendation.length() > 0)
				recommendationsTable.put("spark.python.worker.memory", recommendation);	
		}
		
		//Shuffle Behavior	
		private static void setConsolidateFiles(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			if (inputsTable.get("fileSystem").equals("hdfs")){
				shuffleConsolidateFiles = "false";
			}
			else if (inputsTable.get("fileSystem").equals("ext4") || inputsTable.get("fileSystem").equals("xfs")){
				shuffleConsolidateFiles = "true";
			}
			optionsTable.put("spark.shuffle.consolidateFiles", shuffleConsolidateFiles);
		}
	
		private static void setShuffleManager(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.manager", shuffleManager);
			recommendationsTable.put("spark.shuffle.manager", "Try Hash instead of default Sort based shuffle to try for better performance.");
		}
	
		private static void setShuffleMemoryFraction(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.memoryFraction", shuffleMemoryFraction);
			recommendationsTable.put("spark.shuffle.memoryFraction", "Increase this to a max of 0.8 if there is no RDD caching/persistence in the app");
		}
	
		private static void setShuffleSafetyFraction(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.safetyFraction", shuffleSafetyFraction);
		}
		
		private static void setStorageSafetyFraction(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.storage.safetyFraction", storageSafetyFraction);
		}
		
		private static void setKryoClassesToRegister(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryo.classesToRegister", kryoClassesToRegister);
			recommendationsTable.put("spark.kryo.classesToRegister", "Register classes with Kryo serializer for better performance");
		}
	
		private static void setKyroserializerBufferMax(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryoserializer.buffer.max", kyroserializerBufferMax);
			recommendationsTable.put("spark.kryoserializer.buffer.max", "Increase this if you get a \"buffer limit exceeded\" exception");
		}
	
		private static void setRddCompress(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.rdd.compress", rddCompress);
		}
	
		private static void setSerializer(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			serializer = "org.apache.spark.serializer.KryoSerializer";
			optionsTable.put("spark.serializer", serializer);
			recommendationsTable.put("spark.serializer", "If custom classes are used, MUST register the custom classes to Kyro Serializer. "
					+ "Serialization Debug Info is turned on in Extra Java Options to reflect if class is not registered.");
		}

		private static void setStorageLevel(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			
			double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
			double numNodes = Double.parseDouble(inputsTable.get("numNodes"));
			double memoryPerNode = Double.parseDouble(inputsTable.get("memoryPerNode"));
			double inputDataSize = Double.parseDouble(inputsTable.get("inputDataSize"));
			double storageSafetyFractionValue = Double.parseDouble((storageSafetyFraction));
			double storageMemoryFractionValue = Double.parseDouble((storageMemoryFraction));
			double storageMemoryAvailableFraction = 1 - Double.parseDouble(storageUnrollFraction);
			
			double availableMemory = resourceFraction * (numNodes - 1) * memoryPerNode * storageSafetyFractionValue * storageMemoryFractionValue * storageMemoryAvailableFraction;
			
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
			optionsTable.put("spark.executor.cores", value);
		}
		
		//Not set for now, until optimization
		private static void setDefaultParallelism(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			recommendationsTable.put("spark.default.parallelism","Tune it to 10x the num-cores in the system");
		}
	
		private static void setStorageMemoryFraction(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.storage.memoryFraction", storageMemoryFraction);
			recommendationsTable.put("spark.storage.memoryFraction", "Reduce this to 0.1 if there is no RDD caching/persistence in the app");
		}
	
		private static void setStorageUnrollFraction(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.storage.unrollFraction", storageUnrollFraction);
			recommendationsTable.put("spark.storage.unrollFraction", "Reduce this to 0.1 if there is no RDD caching/persistence in the app");
		}
		
		//Networking
		private static void setAkkaTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.timeout", akkaTimeout);
			recommendationsTable.put("spark.akka.timeout","Increase if GC pauses cause problem");
		}
		
		private static void setCoresMax(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			int allocateCoresMax = Integer.parseInt(executorCores) * Integer.parseInt(executorInstances);
			coresMax = Integer.toString(allocateCoresMax);
			optionsTable.put("spark.cores.max", coresMax);
		}
	
		private static void setLocalityWait(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.locality.wait", localityWait);
			recommendationsTable.put("spark.locality.wait", "Increase this value if long GC pauses");
		}
	
		private static void setSchedulerMinRegisteredResourcesRatio(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.scheduler.minRegisteredResourcesRatio", schedulerMinRegisteredResourcesRatio);
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
			optionsTable.put("spark.speculation", speculation);
			recommendationsTable.put("spark.speculation", "Set to true if stragglers are found");
		}
		
		private static void setDeploySpreadOut(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			//Whether the standalone cluster manager should spread applications out across nodes or try to consolidate them onto as few nodes as possible. Spreading out is usually better for data locality in HDFS, but consolidating is more efficient for compute-intensive workloads. 
			optionsTable.put("spark.deploy.spreadOut", deploySpreadOut);
		}
		
		private static void insertExecutorTotalMaxHeapsizeRecommendation (String recommendation, Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			recommendationsTable.put("SPARK_WORKER_MEMORY","Standalone Mode only: Total amount of memory to allow Spark applications to use on the machine: " + recommendation);
		}
		private static void insertDaemonMasterMaxHeapSizeRecommendation (String recommendation, Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			recommendationsTable.put("SPARK_DAEMON_MEMORY -- MASTER","Standalone Mode only: Total amount of memory to allow Spark applications to use on the machine: " + recommendation);
		}
		private static void insertDaemonWorkerMaxHeapSizeRecommendation (String recommendation, Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			recommendationsTable.put("SPARK_DAEMON_MEMORY -- WORKER","Standalone Mode only: Memory to allocate to the Spark master and worker daemons themselves: " + recommendation);
		}
		
	}
