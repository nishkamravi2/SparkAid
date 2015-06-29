package core.src.main.standalone;
import java.util.*;

public class Standalone {
		/** 
		 * Simple tool to initialize and configure Spark config params, generate the command line and advise
		 * @author Nishkam Ravi (nravi@cloudera.com), Ethan Chan (yish.chan@gmail.com)
		 */
		
		//Application Properties
		static String appName = "";
		static String driverCores = "1";
		static String maxResultSize = "1g"; 
		static String driverMemory = "512m";  
		static String executorMemory = "512m"; 
		static String extraListeners = "";
		static String localDir = "/tmp"; 
		static String logConf = "false";	
		static String master = "";
		
			
		//Runtime Environment
		static String driverExtraClassPath = "";
		static String driverExtraJavaOptions = "";
		static String driverExtraLibraryPath = "";
		static String driverUserClassPathFirst = ""; //false
		static String executorExtraClassPath = "";
		static String executorExtraJavaOptions = "";
		static String executorExtraLibraryPath = "";
		static String executorLogsRollingMaxRetainedFiles = "";
		static String executorLogsRollingMaxSize = "";
		static String executorLogsRollingStrategy = "";
		static String executorLogsRollingTimeInterval = "";
		static String executorUserClassPathFirst = ""; //false
		//array to set all the different executor environment variables e.g: JAVA_HOME, PYSPARK_PYTHON
		static ArrayList<String> executorEnvVariablesArray = new ArrayList<String>();
		//array to set all the default values of executor environment variable values
		static ArrayList<String> executorEnvValuesArray = new ArrayList<String>();
		static String pythonProfile = ""; //false
		static String pythonProfileDump = "";
		static String pythonWorkerMemory = ""; //512m
		static String pythonWorkerReuse = ""; //true
		
		//Shuffle Behavior
		static String reducerMaxSizeInFlight = ""; //48m
		static String shuffleBlockTransferService = ""; //netty
		static String shuffleCompress = ""; //true
		static String consolidateFiles = ""; //false
		static String shuffleFileBuffer = ""; //32k
		static String shuffleIOMaxRetries = ""; //3
		static String shuffleIONumConnectionsPerPeer = ""; //1
		static String shuffleIOPreferDirectBufs = ""; //true
		static String shuffleIORetryWait = ""; //5s
		static String shuffleManager = ""; //sort
		static String shuffleMemoryFraction = ""; //0.2
		static String sortBypassMergeThreshold = ""; //200
		static String shuffleSpill = ""; //true
		static String shuffleSpillCompress = ""; //true
		
		//Spark UI
		static String eventLogCompress = ""; //false
		static String eventLogDir = ""; // "file:///tmp/spark-events"
		static String eventLogEnabled = "false"; //false
		static String uiKillEnabled = ""; //true
		static String uiPort = ""; //4040
		static String retainedJobs = ""; //4040
		static String retainedStages = ""; //1000
		
		//Compression and Serialization
		static String broadcastCompress = ""; //true
		static String closureSerializer = ""; //org.apache.spark.serializer.JavaSerializer
		static String compressionCodec = ""; //snappy
		static String IOCompressionLz4BlockSize = ""; //32k
		static String IOCompressionSnappyBlockSize = ""; //32k
		static String kryoClassesToRegister = "";
		static String kryoReferenceTracking = ""; //true normally else false when using spark SQL thrift server
		static String registrationRequried = ""; //false
		static String kryoRegistrator = "";
		static String kyroserializerBufferMax = ""; //64m
		static String kryoserializerBuffer = ""; //64k
		static String rddCompress = ""; //false
		static String serializer = ""; //org.apache.spark.serializer.JavaSerializer, else org.apache.spark.serializer.KryoSerializer when using Spark SQL Thrift Server
		static String serializerObjectStreamReset = ""; //100
		
		//Execution Behavior
		static String broadCastBlockSize = ""; //4m
		static String broadCastFactory = ""; //org.apache.spark.broadcast.TorrentBroadcastFactory
		static String cleanerTtl = ""; // (infinite)
		static String executorCores = ""; //1 in YARN mode, all the available cores on the worker in standalone mode
		static String defaultParallelism = ""; //local mode: number of cores on local machine, mesos fine grained mode: 8, others: total number of cores on all executor nodes or 2, whichever is larger
		static String executorHeartBeatInterval = ""; //10s
		static String filesFetchTimeout = ""; //60s
		static String filesUseFetchCache = ""; //true
		static String filesOverwrite = ""; //false
		static String hadoopCloneClonf = ""; //false
		static String hadoopValidateOutputSpecs = ""; //true
		static String storageMemoryFraction = ""; //0.6
		static String storageMemoryMapThreshold = ""; //2m
		static String storageUnrollFraction = ""; //0.2
		static String externalBlockStoreBlockManager = ""; //org.apache.spark.storage.TachyonBlockManager
		static String externalBlockStoreBaseDir = ""; //System.getProperty(\"java.io.tmpdir\")
		static String externalBlockStoreURL = ""; //tachyon://localhost:19998 for tachyon
		
		//Networking
		static String akkaFailureDetectorThreshold = ""; //300.0
		static String akkaFrameSize = ""; //10
		static String akkaHeartbeatInterval = ""; //1000s
		static String akkaHeartbeatPauses = ""; //6000s
		static String akkaThreads = ""; //4
		static String akkaTimeout = ""; //100s
		static String blockManagerPort = ""; //random
		static String broadcastPort = ""; //random
		static String driverHost = ""; //local hostname
		static String driverPort = ""; //random
		static String executorPort = ""; //random
		static String fileserverPort = ""; //random
		static String networkTimeout = ""; //120s
		static String portMaxRetries = ""; //16
		static String replClassServerPort = ""; //random
		static String rpcNumRetries = ""; //3
		static String rpcRetryWait = ""; //3s
		static String rpcAskTimeout = ""; //120s
		static String rpcLookupTimeout = ""; //120s
		
		//Scheduling
		static String coresMax = ""; //not set
		static String localExecutionEnabled = ""; //false
		static String localityWait = ""; //3s
		static String localityWaitNode = ""; //Customize the locality wait for node locality
		static String localityWaitProcess = "";
		static String localityWaitRack = "";
		static String schedulerMaxRegisteredResourcesWaitingTime = ""; //30s
		static String schedulerMinRegisteredResourcesRatio = ""; //0.8 for YARN, 0.0 otherwise
		static String schedulerMode = ""; //FIFO
		static String schedulerReviveInterval = ""; //1s
		static String speculation = ""; //false
		static String speculationInterval = ""; //100ms
		static String speculationMultiplier = ""; //1.5
		static String speculationQuantile = ""; //0.75
		static String taskCpus = ""; //1
		static String taskMaxFailures = ""; //4
		
		//Application Settings Methods
		private static void setAppName(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.app.name", appName);
			recommendationsTable.put("spark.app.name", "");
		}
	
		private static void setDriverCores(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.cores", driverCores);
			recommendationsTable.put("spark.driver.cores", "");
			commandLineParamsTable.put("--driver-cores", driverCores);
		}
	
		private static void setMaxResultSize(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.maxResultSize", maxResultSize);
			recommendationsTable.put("spark.driver.maxResultSize", "");
		}
	
		private static void setDriverMemory(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.memory", driverMemory);
			recommendationsTable.put("spark.driver.memory", "");
			//always set --driver-memory
			commandLineParamsTable.put("--driver-memory", driverMemory);
			
		}
	
		private static void setExecutorMemory(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.memory	", executorMemory);
			recommendationsTable.put("spark.executor.memory	", "");
		}
	
		private static void setExtraListeners(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.extraListeners", extraListeners);
			recommendationsTable.put("spark.extraListeners", "");
		}
	
		private static void setLocalDir(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.local.dir", localDir);
			recommendationsTable.put("spark.local.dir", "");
		}
	
		private static void setLogConf(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.logConf", logConf);
			recommendationsTable.put("spark.logConf", "");
		}
	
		private static void setMaster(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.master", master);
			recommendationsTable.put("spark.master", "");
		}
	
		private static void setDriverExtraClassPath(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.extraClassPath", driverExtraClassPath);
			recommendationsTable.put("spark.driver.extraClassPath", "");
			//always set it in terminal
			commandLineParamsTable.put("--driver-class-path", driverExtraClassPath);
		}
	
		private static void setDriverExtraJavaOptions(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.extraJavaOptions", driverExtraJavaOptions);
			recommendationsTable.put("spark.driver.extraJavaOptions", "");
			//always set it in terminal
			commandLineParamsTable.put("--driver-java-options", driverExtraJavaOptions);
		}
	
		private static void setDriverExtraLibraryPath(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.extraLibraryPath", driverExtraLibraryPath);
			recommendationsTable.put("spark.driver.extraLibraryPath", "");
			//always set it in terminal
			commandLineParamsTable.put("--driver-library-path", driverExtraLibraryPath);
		}
	
		private static void setDriverUserClassPathFirst(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.userClassPathFirst", driverUserClassPathFirst);
			recommendationsTable.put("spark.driver.userClassPathFirst", "");
		}
	
		private static void setExecutorExtraClassPath(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.extraClassPath", executorExtraClassPath);
			recommendationsTable.put("spark.executor.extraClassPath", "");
		}
	
		private static void setExecutorExtraJavaOptions(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.extraJavaOptions", executorExtraJavaOptions);
			recommendationsTable.put("spark.executor.extraJavaOptions", "");
		}
	
		private static void setExecutorExtraLibraryPath(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.extraLibraryPath", executorExtraLibraryPath);
			recommendationsTable.put("spark.executor.extraLibraryPath", "");
		}
	
		private static void setExecutorLogsRollingMaxRetainedFiles(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.logs.rolling.maxRetainedFiles", executorLogsRollingMaxRetainedFiles);
			recommendationsTable.put("spark.executor.logs.rolling.maxRetainedFiles", "");
		}
	
		private static void setExecutorLogsRollingMaxSize(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.logs.rolling.maxSize", executorLogsRollingMaxSize);
			recommendationsTable.put("spark.executor.logs.rolling.maxSize", "");
		}
	
		private static void setExecutorLogsRollingStrategy(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.logs.rolling.strategy", executorLogsRollingStrategy);
			recommendationsTable.put("spark.executor.logs.rolling.strategy", "");
		}
	
		private static void setExecutorLogsRollingTimeInterval(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.logs.rolling.time.interval", executorLogsRollingTimeInterval);
			recommendationsTable.put("spark.executor.logs.rolling.time.interval", "");
		}
	
		private static void setExecutorUserClassPathFirst(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.userClassPathFirst", executorUserClassPathFirst);
			recommendationsTable.put("spark.executor.userClassPathFirst", "");
		}
		
		private static void setExecutorEnvEnvironmentVariableName(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			
			for (int i = 0; i < executorEnvVariablesArray.size(); i++){
				String optionVariable = "spark.executorEnv." + executorEnvVariablesArray.get(i);
				optionsTable.put(executorEnvValuesArray.get(i), optionVariable);
				recommendationsTable.put(optionVariable, "");
			}
			
		}
	
		private static void setPythonProfile(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.python.profile", pythonProfile);
			recommendationsTable.put("spark.python.profile", "");
		}
	
		private static void setPythonProfileDump(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.python.profile.dump", pythonProfileDump);
			recommendationsTable.put("spark.python.profile.dump", "");
		}
	
		private static void setPythonWorkerMemory(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.python.worker.memory", pythonWorkerMemory);
			recommendationsTable.put("spark.python.worker.memory", "");
		}
	
		private static void setPythonWorkerReuse(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.python.worker.reuse", pythonWorkerReuse);
			recommendationsTable.put("spark.python.worker.reuse", "");
		}
		
		//Shuffle Behavior
		private static void setReducerMaxSizeInFlight(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.reducer.maxSizeInFlight", reducerMaxSizeInFlight);
			recommendationsTable.put("spark.reducer.maxSizeInFlight", "");
		}
	
		private static void setShuffleBlockTransferService(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.blockTransferService", shuffleBlockTransferService);
			recommendationsTable.put("spark.shuffle.blockTransferService", "");
		}
	
		private static void setShuffleCompress(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.compress", shuffleCompress);
			recommendationsTable.put("spark.shuffle.compress", "");
		}
	
		private static void setConsolidateFiles(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.consolidateFiles", consolidateFiles);
			recommendationsTable.put("spark.shuffle.consolidateFiles", "");
		}
	
		private static void setShuffleFileBuffer(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.file.buffer", shuffleFileBuffer);
			recommendationsTable.put("spark.shuffle.file.buffer", "");
		}
	
		private static void setShuffleIOMaxRetries(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.io.maxRetries", shuffleIOMaxRetries);
			recommendationsTable.put("spark.shuffle.io.maxRetries", "");
		}
	
		private static void setShuffleIONumConnectionsPerPeer(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.io.numConnectionsPerPeer", shuffleIONumConnectionsPerPeer);
			recommendationsTable.put("spark.shuffle.io.numConnectionsPerPeer", "");
		}
	
		private static void setShuffleIOPreferDirectBufs(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.io.preferDirectBufs", shuffleIOPreferDirectBufs);
			recommendationsTable.put("spark.shuffle.io.preferDirectBufs", "");
		}
	
		private static void setShuffleIORetryWait(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.io.retryWait", shuffleIORetryWait);
			recommendationsTable.put("spark.shuffle.io.retryWait", "");
		}
	
		private static void setShuffleManager(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.manager", shuffleManager);
			recommendationsTable.put("spark.shuffle.manager", "");
		}
	
		private static void setShuffleMemoryFraction(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.memoryFraction", shuffleMemoryFraction);
			recommendationsTable.put("spark.shuffle.memoryFraction", "");
		}
	
		private static void setSortBypassMergeThreshold(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.sort.bypassMergeThreshold", sortBypassMergeThreshold);
			recommendationsTable.put("spark.shuffle.sort.bypassMergeThreshold", "");
		}
	
		private static void setShuffleSpill(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.spill", shuffleSpill);
			recommendationsTable.put("spark.shuffle.spill", "");
		}
	
		private static void setShuffleSpillCompress(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.spill.compress", shuffleSpillCompress);
			recommendationsTable.put("spark.shuffle.spill.compress", "");
		}
		
		//Spark UI
		private static void setEventLogCompress(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.eventLog.compress", eventLogCompress);
			recommendationsTable.put("spark.eventLog.compress", "");
		}
	
		private static void setEventLogDir(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.eventLog.dir", eventLogDir);
			recommendationsTable.put("spark.eventLog.dir", "");
		}
	
		private static void setEventLogEnabled(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.eventLog.enabled", eventLogEnabled);
			recommendationsTable.put("spark.eventLog.enabled", "");
		}
	
		private static void setUiKillEnabled(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.ui.killEnabled", uiKillEnabled);
			recommendationsTable.put("spark.ui.killEnabled", "");
		}
	
		private static void setUiPort(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.ui.port", uiPort);
			recommendationsTable.put("spark.ui.port", "");
		}
	
		private static void setRetainedJobs(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.ui.retainedJobs", retainedJobs);
			recommendationsTable.put("spark.ui.retainedJobs", "");
		}
	
		private static void setRetainedStages(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.ui.retainedStages", retainedStages);
			recommendationsTable.put("spark.ui.retainedStages", "");
		}
	
		//Compression and Serialization
		private static void setBroadcastCompress(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.broadcast.compress", broadcastCompress);
			recommendationsTable.put("spark.broadcast.compress", "");
		}
	
		private static void setClosureSerializer(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.closure.serializer", closureSerializer);
			recommendationsTable.put("spark.closure.serializer", "");
		}
	
		private static void setCompressionCodec(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.io.compression.codec", compressionCodec);
			recommendationsTable.put("spark.io.compression.codec", "");
		}
	
		private static void setIOCompressionLz4BlockSize(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.io.compression.lz4.blockSize", IOCompressionLz4BlockSize);
			recommendationsTable.put("spark.io.compression.lz4.blockSize", "");
		}
	
		private static void setIOCompressionSnappyBlockSize(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.io.compression.snappy.blockSize", IOCompressionSnappyBlockSize);
			recommendationsTable.put("spark.io.compression.snappy.blockSize", "");
		}
	
		private static void setKryoClassesToRegister(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryo.classesToRegister", kryoClassesToRegister);
			recommendationsTable.put("spark.kryo.classesToRegister", "");
		}
	
		private static void setKryoReferenceTracking(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryo.referenceTracking", kryoReferenceTracking);
			recommendationsTable.put("spark.kryo.referenceTracking", "");
		}
	
		private static void setKryoRegistrationRequired(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryo.registrationRequired", registrationRequried);
			recommendationsTable.put("spark.kryo.registrationRequired", "");
		}
	
		private static void setKryoRegistrator(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryo.registrator", kryoRegistrator);
			recommendationsTable.put("spark.kryo.registrator", "");
		}
	
		private static void setKyroserializerBufferMax(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryoserializer.buffer.max", kyroserializerBufferMax);
			recommendationsTable.put("spark.kryoserializer.buffer.max", "");
		}
	
		private static void setKryoserializerBuffer(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryoserializer.buffer", kryoserializerBuffer);
			recommendationsTable.put("spark.kryoserializer.buffer", "");
		}
	
		private static void setRddCompress(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.rdd.compress", rddCompress);
			recommendationsTable.put("spark.rdd.compress", "");
		}
	
		private static void setSerializer(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.serializer", serializer);
			recommendationsTable.put("spark.serializer", "");
		}
	
		private static void setSerializerObjectStreamReset(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.serializer.objectStreamReset", serializerObjectStreamReset);
			recommendationsTable.put("spark.serializer.objectStreamReset", "");
		}
		
		//Execution Behavior
		private static void setBroadCastBlockSize(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.broadcast.blockSize", broadCastBlockSize);
			recommendationsTable.put("spark.broadcast.blockSize", "");
		}
	
		private static void setBroadCastFactory(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.broadcast.factory", broadCastFactory);
			recommendationsTable.put("spark.broadcast.factory", "");
		}
	
		private static void setCleanerTtl(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.cleaner.ttl", cleanerTtl);
			recommendationsTable.put("spark.cleaner.ttl", "");
		}
	
		private static void setExecutorCores(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.cores", executorCores);
			recommendationsTable.put("spark.executor.cores", "");
		}
	
		private static void setDefaultParallelism(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.default.parallelism", defaultParallelism);
			recommendationsTable.put("spark.default.parallelism", "");
		}
	
		private static void setExecutorHeartBeatInterval(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.heartbeatInterval", executorHeartBeatInterval);
			recommendationsTable.put("spark.executor.heartbeatInterval", "");
		}
	
		private static void setFilesFetchTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.files.fetchTimeout", filesFetchTimeout);
			recommendationsTable.put("spark.files.fetchTimeout", "");
		}
	
		private static void setFilesUseFetchCache(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.files.useFetchCache", filesUseFetchCache);
			recommendationsTable.put("spark.files.useFetchCache", "");
		}
	
		private static void setFilesOverwrite(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.files.overwrite", filesOverwrite);
			recommendationsTable.put("spark.files.overwrite", "");
		}
	
		private static void setHadoopCloneClonf(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.hadoop.cloneConf", hadoopCloneClonf);
			recommendationsTable.put("spark.hadoop.cloneConf", "");
		}
	
		private static void setHadoopValidateOutputSpecs(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.hadoop.validateOutputSpecs", hadoopValidateOutputSpecs);
			recommendationsTable.put("spark.hadoop.validateOutputSpecs", "");
		}
	
		private static void setStorageMemoryFraction(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.storage.memoryFraction", storageMemoryFraction);
			recommendationsTable.put("spark.storage.memoryFraction", "");
		}
	
		private static void setStorageMemoryMapThreshold(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.storage.memoryMapThreshold", storageMemoryMapThreshold);
			recommendationsTable.put("spark.storage.memoryMapThreshold", "");
		}
	
		private static void setStorageUnrollFraction(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.storage.unrollFraction", storageUnrollFraction);
			recommendationsTable.put("spark.storage.unrollFraction", "");
		}
	
		private static void setExternalBlockStoreBlockManager(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.externalBlockStore.blockManager", externalBlockStoreBlockManager);
			recommendationsTable.put("spark.externalBlockStore.blockManager", "");
		}
	
		private static void setExternalBlockStoreBaseDir(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.externalBlockStore.baseDir", externalBlockStoreBaseDir);
			recommendationsTable.put("spark.externalBlockStore.baseDir", "");
		}
	
		private static void setExternalBlockStoreURL(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.externalBlockStore.url", externalBlockStoreURL);
			recommendationsTable.put("spark.externalBlockStore.url", "");
		}
		
		//Networking
		private static void setAkkaFailureDetectorThreshold(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.failure-detector.threshold", akkaFailureDetectorThreshold);
			recommendationsTable.put("spark.akka.failure-detector.threshold", "");
		}
	
		private static void setAkkaFrameSize(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.frameSize", akkaFrameSize);
			recommendationsTable.put("spark.akka.frameSize", "");
		}
	
		private static void setAkkaHeartbeatInterval(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.heartbeat.interval", akkaHeartbeatInterval);
			recommendationsTable.put("spark.akka.heartbeat.interval", "");
		}
	
		private static void setAkkaHeartbeatPauses(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.heartbeat.pauses", akkaHeartbeatPauses);
			recommendationsTable.put("spark.akka.heartbeat.pauses", "");
		}
	
		private static void setAkkaThreads(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.threads", akkaThreads);
			recommendationsTable.put("spark.akka.threads", "");
		}
	
		private static void setAkkaTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.timeout", akkaTimeout);
			recommendationsTable.put("spark.akka.timeout", "");
		}
	
		private static void setBlockManagerPort(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.blockManager.port", blockManagerPort);
			recommendationsTable.put("spark.blockManager.port", "");
		}
	
		private static void setBroadcastPort(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.broadcast.port", broadcastPort);
			recommendationsTable.put("spark.broadcast.port", "");
		}
	
		private static void setDriverHost(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.host", driverHost);
			recommendationsTable.put("spark.driver.host", "");
		}
	
		private static void setDriverPort(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.port", driverPort);
			recommendationsTable.put("spark.driver.port", "");
		}
	
		private static void setExecutorPort(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.por", executorPort);
			recommendationsTable.put("spark.executor.por", "");
		}
	
		private static void setFileserverPort(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.fileserver.port", fileserverPort);
			recommendationsTable.put("spark.fileserver.port", "");
		}
	
		private static void setNetworkTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.network.timeout", networkTimeout);
			recommendationsTable.put("spark.network.timeout", "");
		}
	
		private static void setPortMaxRetries(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.port.maxRetries", portMaxRetries);
			recommendationsTable.put("spark.port.maxRetries", "");
		}
	
		private static void setReplClassServerPort(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.replClassServer.port", replClassServerPort);
			recommendationsTable.put("spark.replClassServer.port", "");
		}
	
		private static void setRpcNumRetries(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.rpc.numRetries", rpcNumRetries);
			recommendationsTable.put("spark.rpc.numRetries", "");
		}
	
		private static void setRpcRetryWait(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.rpc.retry.wait", rpcRetryWait);
			recommendationsTable.put("spark.rpc.retry.wait", "");
		}
	
		private static void setRpcAskTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.rpc.askTimeout", rpcAskTimeout);
			recommendationsTable.put("spark.rpc.askTimeout", "");
		}
	
		private static void setRpcLookupTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.rpc.lookupTimeout", rpcLookupTimeout);
			recommendationsTable.put("spark.rpc.lookupTimeout", "");
		}
		
		//Scheduling
		private static void setCoresMax(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.cores.max", coresMax);
			recommendationsTable.put("spark.cores.max", "");
		}
	
		private static void setLocalExecutionEnabled(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.localExecution.enabled", localExecutionEnabled);
			recommendationsTable.put("spark.localExecution.enabled", "");
		}
	
		private static void setLocalityWait(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.locality.wait", localityWait);
			recommendationsTable.put("spark.locality.wait", "");
		}
	
		private static void setLocalityWaitNode(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.locality.wait.node", localityWaitNode);
			recommendationsTable.put("spark.locality.wait.node", "");
		}
	
		private static void setLocalityWaitProcess(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.locality.wait.process", localityWaitProcess);
			recommendationsTable.put("spark.locality.wait.process", "");
		}
	
		private static void setLocalityWaitRack(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.locality.wait.rack", localityWaitRack);
			recommendationsTable.put("spark.locality.wait.rack", "");
		}
	
		private static void setSchedulerMaxRegisteredResourcesWaitingTime(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.scheduler.maxRegisteredResourcesWaitingTime", schedulerMaxRegisteredResourcesWaitingTime);
			recommendationsTable.put("spark.scheduler.maxRegisteredResourcesWaitingTime", "");
		}
	
		private static void setSchedulerMinRegisteredResourcesRatio(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.scheduler.minRegisteredResourcesRatio", schedulerMinRegisteredResourcesRatio);
			recommendationsTable.put("spark.scheduler.minRegisteredResourcesRatio", "");
		}
	
		private static void setSchedulerMode(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.scheduler.mode", schedulerMode);
			recommendationsTable.put("spark.scheduler.mode", "");
		}
	
		private static void setSchedulerReviveInterval(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.scheduler.revive.interval", schedulerReviveInterval);
			recommendationsTable.put("spark.scheduler.revive.interval", "");
		}
	
		private static void setSpeculation(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.speculation", speculation);
			recommendationsTable.put("spark.speculation", "");
		}
	
		private static void setSpeculationInterval(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.speculation.interval", speculationInterval);
			recommendationsTable.put("spark.speculation.interval", "");
		}
	
		private static void setSpeculationMultiplier(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.speculation.multiplier", speculationMultiplier);
			recommendationsTable.put("spark.speculation.multiplier", "");
		}
	
		private static void setSpeculationQuantile(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.speculation.quantile", speculationQuantile);
			recommendationsTable.put("spark.speculation.quantile", "");
		}
	
		private static void setTaskCpus(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.task.cpus", taskCpus);
			recommendationsTable.put("spark.task.cpus", "");
		}
	
		private static void setTaskMaxFailures(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.task.maxFailures", taskMaxFailures);
			recommendationsTable.put("spark.task.maxFailures", "");
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
