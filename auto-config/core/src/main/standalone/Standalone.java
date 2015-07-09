 package core.src.main.standalone;
import java.util.*;

import utils.UtilsConversion;

public class Standalone {
	
		//must have settings through heuristics
		static String driverMemory = ""; 
		static String executorMemory = ""; 
		static String driverCores = "";
		static String executorCores = ""; 
		static String schedulerMode = "";
		static String coresMax = ""; 
		static String shuffleConsolidateFiles = "";
		static String defaultParallelism = "";
		static String rddCompress = "";
		static String storageLevel = "";
		
		//Default Values
		
		//Application Properties
		static String appName = ""; //given at command line
		static String driverMaxResultSize = "0";
		static String extraListeners = "";
		static String localDir = "/tmp";
		static String logConf = "false";
		static String master = ""; //given at command line
		
		//RunTime
		static String driverExtraClassPath = ""; //none
		static String driverExtraJavaOptions = "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps";
		static String driverExtraLibraryPath = ""; //none
		static String driverUserClassPathFirst = "false"; 
		static String executorExtraClassPath = ""; //none
		static String executorExtraJavaOptions = "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps";
		static String executorExtraLibraryPath = ""; //none
		static String executorLogsRollingMaxRetainedFiles = ""; //none
		static String executorLogsRollingMaxSize = ""; //none
		static String executorLogsRollingStrategy = ""; //none
		static String executorLogsRollingTimeInterval = "daily";
		static String executorUserClassPathFirst = "false"; 
		static ArrayList<String> executorEnvValuesArray = new ArrayList<String>();
		static ArrayList<String> executorEnvVariablesArray = new ArrayList<String>();
		static String pythonProfile = "false";
		static String pythonProfileDump = ""; //none
		static String pythonWorkerMemory = "512m"; //Uses same heuristic as Executor Memory
		static String pythonWorkerReuse = "true";
		
		//Shuffle
		static String reducerMaxSizeInFlight = "48m";
		static String shuffleBlockTransferService = "netty";
		static String shuffleCompress = "true";
		static String shuffleFileBuffer = "32k"; 
		static String shuffleIOMaxRetries = "3"; 
		static String shuffleIONumConnectionsPerPeer = "1"; 
		static String shuffleIOPreferDirectBufs = "true"; 
		static String shuffleIORetryWait = "5"; //s
		static String shuffleManager = "sort";
		static String shuffleMemoryFraction = "0.2"; // storageMemoryFraction + storageUnrollFraction + shuffleMemoryFraction = 1
		static String shuffleSafetyFraction = "0.8"; //Not in documentation
		static String sortBypassMergeThreshold = "200"; // partitions
		static String shuffleSpill = "true";
		static String shuffleSpillCompress = "true";
		
		//Spark UI
		static String eventLogCompress = "false"; 
		static String eventLogDir = ""; // "file:///tmp/spark-events" //set a directory here
		static String eventLogEnabled = "false"; 
		static String uiKillEnabled = "true"; 
		static String uiPort = "4040";
		static String uiRetainedJobs = "1000";
		static String uiRetainedStages = "1000"; 
		
		//Compression and Serialization
		static String broadcastCompress = "true"; 
		static String closureSerializer = "org.apache.spark.serializer.JavaSerializer";
		static String compressionCodec = "snappy"; 
		static String IOCompressionLz4BlockSize = "32k";
		static String IOCompressionSnappyBlockSize = "32k"; 
		static String kryoClassesToRegister = ""; //insert kryo classes to register here
		static String kryoReferenceTracking = "true"; //true normally else false when using spark SQL thrift server
		static String kryoRegistrationRequried = "false";
		static String kryoRegistrator = "";
		static String kryoserializerBuffer = "64k"; 
		static String kyroserializerBufferMax = "64m";
		static String serializer = "org.apache.spark.serializer.KryoSerializer"; //org.apache.spark.serializer.JavaSerializer, else org.apache.spark.serializer.KryoSerializer when using Spark SQL Thrift Server
		static String serializerObjectStreamReset = "100"; 
		
		//Execution Behaviour
		static String broadCastBlockSize = "4000"; //kb  
		static String broadCastFactory = "org.apache.spark.broadcast.TorrentBroadcastFactory";
		static String cleanerTtl = ""; // (infinite)
		static String executorHeartBeatInterval = "10000"; //ms
		static String filesFetchTimeout = "60"; //s
		static String filesOverwrite = "false"; 
		static String filesUseFetchCache = "true";
		static String hadoopCloneClonf = "false"; 
		static String hadoopValidateOutputSpecs = "true";
		static String storageMemoryFraction = "0.6"; // storageMemoryFraction + storageUnrollFraction + shuffleMemoryFraction = 1
		static String storageMemoryMapThreshold = "2097152"; //2 * 1024 * 1024 bytes
		static String storageSafetyFraction = "0.9"; //not in Spark documentation
		static String storageUnrollFraction = "0.2"; // storageMemoryFraction + storageUnrollFraction + shuffleMemoryFraction = 1
		static String externalBlockStoreBlockManager = "org.apache.spark.storage.TachyonBlockManager";
		static String externalBlockStoreBaseDir = ""; //System.getProperty(\"java.io.tmpdir\")
		static String externalBlockStoreURL = ""; //tachyon://localhost:19998 for tachyon
		
		//Networking
		static String akkaFailureDetectorThreshold = "300.0"; //300.0
		static String akkaFrameSize = "10"; //MB
		static String akkaHeartbeatInterval = "1000"; //s
		static String akkaHeartbeatPauses = "6000"; //s
		static String akkaThreads = "4"; //
		static String akkaTimeout = "100"; //s
		static String blockManagerPort = ""; //random
		static String broadcastPort = ""; //random
		static String driverHost = ""; //local hostname
		static String driverPort = ""; //random
		static String executorPort = ""; //random
		static String fileserverPort = ""; //random
		static String networkTimeout = "120"; //120s
		static String portMaxRetries = ""; //16
		static String replClassServerPort = ""; //random
		static String rpcNumRetries = "3"; //3
		static String rpcRetryWait = "3000"; //ms
		static String rpcAskTimeout = "120"; //120s
		static String rpcLookupTimeout = "120"; //120s
		
		//Scheduling
		static String localExecutionEnabled = "false"; 
		static String localityWait = "3000"; //ms
		static String localityWaitNode = ""; //follows spark.locality.wait
		static String localityWaitProcess = ""; //follows spark.locality.wait
		static String localityWaitRack = ""; //follows spark.locality.wait
		static String schedulerMaxRegisteredResourcesWaitingTime = "30000"; //ms
		static String schedulerMinRegisteredResourcesRatio = "0.0"; //0.8 for YARN, 0.0 otherwise
		static String schedulerReviveInterval = "1000"; //ms
		static String speculation = "false"; //false
		static String speculationInterval = "100"; //ms
		static String speculationMultiplier = "1.5"; //1.5
		static String speculationQuantile = "0.75"; //0.75
		static String taskCpus = "1";
		static String taskMaxFailures = "4";
		
		
		
		//Application Settings Methods
		private static void setAppName(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.app.name", appName);
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
	
		private static void setDriverMemory(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
			double memoryPerNode = UtilsConversion.parseMemory(inputsTable.get("memoryPerNode")); //in mb
			//Set driver memory 0.8 of current node's memory 
			double targetDriverMemory = memoryPerNode * 0.8 * resourceFraction;
			driverMemory = Integer.toString((int)targetDriverMemory) + "g";
			optionsTable.put("spark.driver.memory", driverMemory);
			commandLineParamsTable.put("--driver-memory", driverMemory);
		}
	
		private static void setExecutorMemory(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			//assumption is that there is only one executor per node in standalone 1.3.0
			double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
			double memoryPerNode = UtilsConversion.parseMemory(inputsTable.get("memoryPerNode"));
			double availableMemoryPerNode = resourceFraction * memoryPerNode;
			//general heuristic, want a min 512 mb, and a max of 64 gb of JVM
			double targetMemoryPerNode = 0.0;
			if (availableMemoryPerNode > 0.6){
				targetMemoryPerNode =  0.9 *availableMemoryPerNode;
				System.out.println(targetMemoryPerNode);
			}
			if (targetMemoryPerNode > 64){
				targetMemoryPerNode = 64;
			}
			
			executorMemory = String.valueOf((int)targetMemoryPerNode) + "g";
			optionsTable.put("spark.executor.memory", executorMemory);
			recommendationsTable.put("spark.executor.memory", "Assumption: Only one executor per node in standalone 1.3.0");
		}
	
		private static void setExtraListeners(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.extraListeners", extraListeners);
		}
	
		private static void setLocalDir(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.local.dir", localDir);
			recommendationsTable.put("spark.local.dir", "Ensure this location has TB's of space");
		}
	
		private static void setLogConf(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.logConf", logConf);
		}
	
		private static void setMaster(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.master", master);
		}
	
		private static void setDriverExtraClassPath(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.extraClassPath", driverExtraClassPath);
		}
	
		private static void setDriverExtraJavaOptions(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.extraJavaOptions", driverExtraJavaOptions);
			recommendationsTable.put("spark.driver.extraJavaOptions", "In case of long gc pauses, try adding the following: -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled");
		}
	
		private static void setDriverExtraLibraryPath(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.extraLibraryPath", driverExtraLibraryPath);
		}
	
		private static void setDriverUserClassPathFirst(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.userClassPathFirst", driverUserClassPathFirst);
		}
	
		private static void setExecutorExtraClassPath(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.extraClassPath", executorExtraClassPath);
		}
	
		private static void setExecutorExtraJavaOptions(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.extraJavaOptions", executorExtraJavaOptions);
			recommendationsTable.put("spark.executor.extraJavaOptions", "In case of long gc pauses, try adding the following: -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled");
		}
	
		private static void setExecutorExtraLibraryPath(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.extraLibraryPath", executorExtraLibraryPath);
		}
	
		private static void setExecutorLogsRollingMaxRetainedFiles(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.logs.rolling.maxRetainedFiles", executorLogsRollingMaxRetainedFiles);
		}
	
		private static void setExecutorLogsRollingMaxSize(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.logs.rolling.maxSize", executorLogsRollingMaxSize);
		}
	
		private static void setExecutorLogsRollingStrategy(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.logs.rolling.strategy", executorLogsRollingStrategy);
		}
	
		private static void setExecutorLogsRollingTimeInterval(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.logs.rolling.time.interval", executorLogsRollingTimeInterval);
		}
	
		private static void setExecutorUserClassPathFirst(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.userClassPathFirst", executorUserClassPathFirst);
		}
		
		private static void setExecutorEnvEnvironmentVariableName(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			
			for (int i = 0; i < executorEnvVariablesArray.size(); i++){
				String optionVariable = "spark.executorEnv." + executorEnvVariablesArray.get(i);
				optionsTable.put(executorEnvValuesArray.get(i), optionVariable);
			}
			
		}
	
		private static void setPythonProfile(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.python.profile", pythonProfile);
		}
	
		private static void setPythonProfileDump(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.python.profile.dump", pythonProfileDump);
			recommendationsTable.put("spark.python.profile.dump","Set directory to dump profile result before driver exiting if desired");
		}
	
		private static void setPythonWorkerMemory(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			//assumption is that there is only one executor per node in standalone 1.3.0
			double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
			double memoryPerNode = UtilsConversion.parseMemory(inputsTable.get("memoryPerNode"));
			double availableMemoryPerNode = resourceFraction * memoryPerNode;
			//general heuristic, want a min 512 mb, and a max of 64 gb of JVM
			double targetMemoryPerNode = 0.0;
			if (availableMemoryPerNode > 0.6){
				targetMemoryPerNode =  0.9 *availableMemoryPerNode;
				System.out.println(targetMemoryPerNode);
			}
			if (targetMemoryPerNode > 64){
				targetMemoryPerNode = 64;
			}
			
			pythonWorkerMemory = String.valueOf((int)targetMemoryPerNode) + "g";
			optionsTable.put("spark.python.worker.memory", pythonWorkerMemory);
			recommendationsTable.put("spark.python.worker.memory", "Assumption: Only one executor per node in standalone 1.3.0");
		}
	
		private static void setPythonWorkerReuse(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.python.worker.reuse", pythonWorkerReuse);
		}
		
		//Shuffle Behavior
		private static void setReducerMaxSizeInFlight(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.reducer.maxSizeInFlight", reducerMaxSizeInFlight);
		}
	
		private static void setShuffleBlockTransferService(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.blockTransferService", shuffleBlockTransferService);
		}
	
		private static void setShuffleCompress(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.compress", shuffleCompress);
		}
	
		private static void setConsolidateFiles(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			if (inputsTable.get("fileSystem").equals("hdfs")){
				shuffleConsolidateFiles = "false";
			}
			else if (inputsTable.get("fileSystem").equals("ext4") || inputsTable.get("fileSystem").equals("xfs")){
				shuffleConsolidateFiles = "true";
			}
			optionsTable.put("spark.shuffle.consolidateFiles", shuffleConsolidateFiles);
		}
	
		private static void setShuffleFileBuffer(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.file.buffer", shuffleFileBuffer);
			recommendationsTable.put("spark.shuffle.file.buffer", "Increase this value to improve shuffle performance when a lot of memory is available");
		}
	
		private static void setShuffleIOMaxRetries(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.io.maxRetries", shuffleIOMaxRetries);
		}
	
		private static void setShuffleIONumConnectionsPerPeer(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.io.numConnectionsPerPeer", shuffleIONumConnectionsPerPeer);
		}
	
		private static void setShuffleIOPreferDirectBufs(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.io.preferDirectBufs", shuffleIOPreferDirectBufs);
		}
	
		private static void setShuffleIORetryWait(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.io.retryWait", shuffleIORetryWait);
		}
	
		private static void setShuffleManager(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.manager", shuffleManager);
		}
	
		private static void setShuffleMemoryFraction(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.memoryFraction", shuffleMemoryFraction);
			recommendationsTable.put("spark.shuffle.memoryFraction", "Increase this to 0.8 if there is no RDD caching/persistence in the app");
		}
	
		private static void setSortBypassMergeThreshold(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.sort.bypassMergeThreshold", sortBypassMergeThreshold);
		}
	
		private static void setShuffleSpill(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.spill", shuffleSpill);
		}
	
		private static void setShuffleSpillCompress(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.spill.compress", shuffleSpillCompress);
		}
		
		private static void setShuffleSafetyFraction(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.safetyFraction", shuffleSafetyFraction);
		}
		
		private static void setStorageSafetyFraction(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.storage.safetyFraction", storageSafetyFraction);
		}
		
		
		//Spark UI
		private static void setEventLogCompress(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.eventLog.compress", eventLogCompress);
		}
	
		private static void setEventLogDir(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.eventLog.dir", eventLogDir);
		}
	
		private static void setEventLogEnabled(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.eventLog.enabled", eventLogEnabled);
		}
	
		private static void setUiKillEnabled(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.ui.killEnabled", uiKillEnabled);
		}
	
		private static void setUiPort(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.ui.port", uiPort);
		}
	
		private static void setRetainedJobs(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.ui.retainedJobs", uiRetainedJobs);
		}
	
		private static void setRetainedStages(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.ui.retainedStages", uiRetainedStages);
		}
	
		//Compression and Serialization
		private static void setBroadcastCompress(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.broadcast.compress", broadcastCompress);
		}
	
		private static void setClosureSerializer(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.closure.serializer", closureSerializer);
		}
	
		private static void setCompressionCodec(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.io.compression.codec", compressionCodec);
		}
	
		private static void setIOCompressionLz4BlockSize(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.io.compression.lz4.blockSize", IOCompressionLz4BlockSize);
		}
	
		private static void setIOCompressionSnappyBlockSize(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.io.compression.snappy.blockSize", IOCompressionSnappyBlockSize);
		}
	
		private static void setKryoClassesToRegister(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryo.classesToRegister", kryoClassesToRegister);
		}
	
		private static void setKryoReferenceTracking(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryo.referenceTracking", kryoReferenceTracking);
		}
	
		private static void setKryoRegistrationRequired(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryo.registrationRequired", kryoRegistrationRequried);
		}
	
		private static void setKryoRegistrator(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryo.registrator", kryoRegistrator);
		}
	
		private static void setKyroserializerBufferMax(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryoserializer.buffer.max", kyroserializerBufferMax);
			recommendationsTable.put("spark.kryoserializer.buffer.max", "Increase this if you get a \"buffer limit exceeded\" exception");
		}
	
		private static void setKryoserializerBuffer(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryoserializer.buffer", kryoserializerBuffer);
		}
	
		//check ifit needs compression
		//This will only work for MEMORY_ONLY_SER 
		private static void setRddCompress(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			
			double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
			double numNodes = Double.parseDouble(inputsTable.get("numNodes"));
			double memoryPerNode = Double.parseDouble(inputsTable.get("memoryPerNode"));
			double inputDataSize = Double.parseDouble(inputsTable.get("inputDataSize"));
			double storageSafetyFractionValue = Double.parseDouble((storageSafetyFraction));
			double storageMemoryFractionValue = Double.parseDouble((storageMemoryFraction));
			
			double availableMemory = resourceFraction * (numNodes - 1)* memoryPerNode * storageSafetyFractionValue * storageMemoryFractionValue;
			//Based on experiments, serialized data is 1.5x the size of hdfs raw data
			double expectedMemory = inputDataSize * 2;
			
			double ratio = availableMemory / expectedMemory;
			if (ratio <= 1){
				rddCompress = "true";
			}
			else{
				rddCompress = "false";
			}
			
			optionsTable.put("spark.rdd.compress", rddCompress);
			recommendationsTable.put("spark.rdd.compress", "Ensure persist() level is MEMORY_ONLY_SER.");
			
		}
	
		private static void setSerializer(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.serializer", serializer);
			recommendationsTable.put("spark.serializer", "If custom classes are used, MUST register the custom classes to Kyro Serializer. "
					+ "Serialization Debug Info is turned on in Extra Java Options to reflect if class is not registered.");
		}
	
		private static void setSerializerObjectStreamReset(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.serializer.objectStreamReset", serializerObjectStreamReset);
		}
		
		private static void setStorageLevel(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			
			double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
			double numNodes = Double.parseDouble(inputsTable.get("numNodes"));
			double memoryPerNode = Double.parseDouble(inputsTable.get("memoryPerNode"));
			double inputDataSize = Double.parseDouble(inputsTable.get("inputDataSize"));
			double storageSafetyFractionValue = Double.parseDouble((storageSafetyFraction));
			double storageMemoryFractionValue = Double.parseDouble((storageMemoryFraction));
			
			double availableMemory = resourceFraction * (numNodes - 1) * memoryPerNode * storageSafetyFractionValue * storageMemoryFractionValue;
			//Based on experiments, serialized data is 1.5x the size of hdfs raw data
			double expectedMemory = inputDataSize * 2;
			
			double ratio = availableMemory / expectedMemory;
			
			//Based from SparkAutoConfig 1.0, might need to revisit.
			if (ratio > 3){
				storageLevel = "MEMORY_ONLY";
			}else if(ratio > 2){
				storageLevel = "MEMORY_AND_DISK";
			}else{
				storageLevel = "MEMORY_AND_DISK_SER";
			}
			
			optionsTable.put("spark.storage.level", storageLevel);
			
		}
		
		//Execution Behavior
		private static void setBroadCastBlockSize(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.broadcast.blockSize", broadCastBlockSize);
		}
	
		private static void setBroadCastFactory(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.broadcast.factory", broadCastFactory);
		}
	
		private static void setCleanerTtl(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.cleaner.ttl", cleanerTtl);
		}
	
		private static void setExecutorCores(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			//assumption is that there is only one executor per node in standalone 1.3.0
			double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
			double numCoresPerNode = Integer.parseInt(inputsTable.get("numCoresPerNode"));
			int targetExecutorCores = (int)(resourceFraction * numCoresPerNode);
			executorCores = String.valueOf(targetExecutorCores);
			optionsTable.put("spark.executor.cores", executorCores);
		}
	
		private static void setDefaultParallelism(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			//for the the default parallelism is set to 2 * the number of cores
			double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
			double numNodes = Double.parseDouble(inputsTable.get("numNodes"));
			double coresPerNode = Double.parseDouble(inputsTable.get("numCoresPerNode")); //in mb
			double numWorkerNodes = numNodes - 1;
			defaultParallelism = String.valueOf((int)(coresPerNode * numWorkerNodes * resourceFraction * 2));
			optionsTable.put("spark.default.parallelism", defaultParallelism);
		}
	
		private static void setExecutorHeartBeatInterval(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.heartbeatInterval", executorHeartBeatInterval);
		}
	
		private static void setFilesFetchTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.files.fetchTimeout", filesFetchTimeout);
		}
	
		private static void setFilesUseFetchCache(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.files.useFetchCache", filesUseFetchCache);
		}
	
		private static void setFilesOverwrite(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.files.overwrite", filesOverwrite);
		}
	
		private static void setHadoopCloneClonf(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.hadoop.cloneConf", hadoopCloneClonf);
		}
	
		private static void setHadoopValidateOutputSpecs(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.hadoop.validateOutputSpecs", hadoopValidateOutputSpecs);
		}
	
		private static void setStorageMemoryFraction(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.storage.memoryFraction", storageMemoryFraction);
			recommendationsTable.put("spark.storage.memoryFraction", "Reduce this to 0.1 if there is no RDD caching/persistence in the app");
		}
	
		private static void setStorageMemoryMapThreshold(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.storage.memoryMapThreshold", storageMemoryMapThreshold);
		}
	
		private static void setStorageUnrollFraction(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.storage.unrollFraction", storageUnrollFraction);
			recommendationsTable.put("spark.storage.unrollFraction", "Reduce this to 0.1 if there is no RDD caching/persistence in the app");
		}
	
		private static void setExternalBlockStoreBlockManager(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.externalBlockStore.blockManager", externalBlockStoreBlockManager);
		}
	
		private static void setExternalBlockStoreBaseDir(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.externalBlockStore.baseDir", externalBlockStoreBaseDir);
		}
	
		private static void setExternalBlockStoreURL(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.externalBlockStore.url", externalBlockStoreURL);
		}
		
		//Networking
		private static void setAkkaFailureDetectorThreshold(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.failure-detector.threshold", akkaFailureDetectorThreshold);
		}
	
		private static void setAkkaFrameSize(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.frameSize", akkaFrameSize);
		}
	
		private static void setAkkaHeartbeatInterval(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.heartbeat.interval", akkaHeartbeatInterval);
		}
	
		private static void setAkkaHeartbeatPauses(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.heartbeat.pauses", akkaHeartbeatPauses);
		}
	
		private static void setAkkaThreads(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.threads", akkaThreads);
		}
	
		private static void setAkkaTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.timeout", akkaTimeout);
			recommendationsTable.put("spark.akka.timeout","Increase if GC pauses cause problem");
		}
	
		private static void setBlockManagerPort(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.blockManager.port", blockManagerPort);
		}
	
		private static void setBroadcastPort(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.broadcast.port", broadcastPort);
		}
	
		private static void setDriverHost(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.host", driverHost);
		}
	
		private static void setDriverPort(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.port", driverPort);
		}
	
		private static void setExecutorPort(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.por", executorPort);
		}
	
		private static void setFileserverPort(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.fileserver.port", fileserverPort);
		}
	
		private static void setNetworkTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.network.timeout", networkTimeout);
		}
	
		private static void setPortMaxRetries(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.port.maxRetries", portMaxRetries);
		}
	
		private static void setReplClassServerPort(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.replClassServer.port", replClassServerPort);
		}
	
		private static void setRpcNumRetries(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.rpc.numRetries", rpcNumRetries);
		}
	
		private static void setRpcRetryWait(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.rpc.retry.wait", rpcRetryWait);
		}
	
		private static void setRpcAskTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.rpc.askTimeout", rpcAskTimeout);
		}
	
		private static void setRpcLookupTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.rpc.lookupTimeout", rpcLookupTimeout);
		}
		
		//Scheduling
		private static void setCoresMax(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			double resourceFraction = Double.parseDouble(inputsTable.get("resourceFraction"));
			String numCoresPerNode = inputsTable.get("numCoresPerNode");
			double effectiveCoresPerNode = resourceFraction * Double.parseDouble(numCoresPerNode);
			double numNodes = Double.parseDouble(inputsTable.get("numNodes"));
			coresMax = String.valueOf((int)(numNodes * effectiveCoresPerNode));
			optionsTable.put("spark.cores.max", coresMax);
		}
	
		private static void setLocalExecutionEnabled(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.localExecution.enabled", localExecutionEnabled);
		}
	
		private static void setLocalityWait(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.locality.wait", localityWait);
			recommendationsTable.put("spark.locality.wait", "Increase this value if long GC pauses");
		}
	
		private static void setLocalityWaitNode(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.locality.wait.node", localityWaitNode);
		}
	
		private static void setLocalityWaitProcess(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.locality.wait.process", localityWaitProcess);
		}
	
		private static void setLocalityWaitRack(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.locality.wait.rack", localityWaitRack);
		}
	
		private static void setSchedulerMaxRegisteredResourcesWaitingTime(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.scheduler.maxRegisteredResourcesWaitingTime", schedulerMaxRegisteredResourcesWaitingTime);
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
	
		private static void setSchedulerReviveInterval(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.scheduler.revive.interval", schedulerReviveInterval);
		}
	
		private static void setSpeculation(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.speculation", speculation);
			recommendationsTable.put("spark.speculation", "Set to true if stragglers are found");
		}
	
		private static void setSpeculationInterval(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.speculation.interval", speculationInterval);
		}
	
		private static void setSpeculationMultiplier(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.speculation.multiplier", speculationMultiplier);
		}
	
		private static void setSpeculationQuantile(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.speculation.quantile", speculationQuantile);
		}
	
		private static void setTaskCpus(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.task.cpus", taskCpus);
		}
	
		private static void setTaskMaxFailures(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.task.maxFailures", taskMaxFailures);
		}

		public static void setNetworking(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			setAkkaFailureDetectorThreshold(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setAkkaFrameSize(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setAkkaHeartbeatInterval(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setAkkaHeartbeatPauses(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setAkkaThreads(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setAkkaTimeout(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setBlockManagerPort(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setBroadcastPort(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setDriverHost(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setDriverPort(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorPort(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setFileserverPort(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setNetworkTimeout(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setPortMaxRetries(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setReplClassServerPort(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setRpcNumRetries(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setRpcRetryWait(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setRpcAskTimeout(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setRpcLookupTimeout(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setCoresMax(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setLocalExecutionEnabled(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setLocalityWait(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setLocalityWaitNode(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setLocalityWaitProcess(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setLocalityWaitRack(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSchedulerMaxRegisteredResourcesWaitingTime(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSchedulerMinRegisteredResourcesRatio(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSchedulerMode(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSchedulerReviveInterval(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSpeculation(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSpeculationInterval(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSpeculationMultiplier(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSpeculationQuantile(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setTaskCpus(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setTaskMaxFailures(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}

		public static void setExecutionBehavior(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			setBroadCastBlockSize(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setBroadCastFactory(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setCleanerTtl(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorCores(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setDefaultParallelism(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorHeartBeatInterval(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setFilesFetchTimeout(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setFilesUseFetchCache(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setFilesOverwrite(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setHadoopCloneClonf(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setHadoopValidateOutputSpecs(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setStorageMemoryFraction(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setStorageMemoryMapThreshold(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setStorageUnrollFraction(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExternalBlockStoreBlockManager(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExternalBlockStoreBaseDir(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExternalBlockStoreURL(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}

		public static void setCompressionSerialization(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			setBroadcastCompress(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setClosureSerializer(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setCompressionCodec(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setIOCompressionLz4BlockSize(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setIOCompressionSnappyBlockSize(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setKryoClassesToRegister(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setKryoReferenceTracking(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setKryoRegistrationRequired(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setKryoRegistrator(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setKyroserializerBufferMax(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setKryoserializerBuffer(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setRddCompress(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSerializer(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSerializerObjectStreamReset(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setStorageLevel(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}

		public static void setSparkUI(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			setEventLogCompress(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setEventLogDir(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setEventLogEnabled(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setUiKillEnabled(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setUiPort(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setRetainedJobs(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setRetainedStages(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}

		public static void setShuffleBehavior(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			setReducerMaxSizeInFlight(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleBlockTransferService(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleCompress(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setConsolidateFiles(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleFileBuffer(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleIOMaxRetries(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleIONumConnectionsPerPeer(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleIOPreferDirectBufs(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleIORetryWait(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleManager(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleMemoryFraction(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSortBypassMergeThreshold(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleSpill(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleSpillCompress(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleSafetyFraction(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setStorageSafetyFraction(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    
		}

		public static void setRunTimeEnvironment(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			setDriverExtraClassPath(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setDriverExtraJavaOptions(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setDriverExtraLibraryPath(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setDriverUserClassPathFirst(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorExtraClassPath(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorExtraJavaOptions(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorExtraLibraryPath(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorLogsRollingMaxRetainedFiles(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorLogsRollingMaxSize(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorLogsRollingStrategy(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorLogsRollingTimeInterval(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorUserClassPathFirst(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorEnvEnvironmentVariableName(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setPythonProfile(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setPythonProfileDump(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setPythonWorkerMemory(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setPythonWorkerReuse(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}

		public static void setApplicationProperties(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			setAppName(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setDriverCores(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setMaxResultSize(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setDriverMemory(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorMemory(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExtraListeners(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setLocalDir(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setLogConf(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		    setMaster(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}

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
			// Set Spark UI
			setSparkUI(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			// Set Compression and Serialization
			setCompressionSerialization(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			// Set Execution Behavior
			setExecutionBehavior(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			// Set Networking
			setNetworking(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);

		}
	}
