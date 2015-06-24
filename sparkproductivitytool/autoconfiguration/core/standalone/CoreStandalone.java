package core.standalone;
import java.util.*;

public class CoreStandalone {
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
		private static void setAppName(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.app.name", appName);
			recommendationsTable.put("spark.app.name", "default recommendation");
		}
	
		private static void setDriverCores(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.cores", driverCores);
			recommendationsTable.put("spark.driver.cores", "default recommendation");
		}
	
		private static void setMaxResultSize(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.maxResultSize", maxResultSize);
			recommendationsTable.put("spark.driver.maxResultSize", "default recommendation");
		}
	
		private static void setDriverMemory(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.memory", driverMemory);
			recommendationsTable.put("spark.driver.memory", "default recommendation");
		}
	
		private static void setExecutorMemory(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.memory	", executorMemory);
			recommendationsTable.put("spark.executor.memory	", "default recommendation");
		}
	
		private static void setExtraListeners(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.extraListeners", extraListeners);
			recommendationsTable.put("spark.extraListeners", "default recommendation");
		}
	
		private static void setLocalDir(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.local.dir", localDir);
			recommendationsTable.put("spark.local.dir", "default recommendation");
		}
	
		private static void setLogConf(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.logConf", logConf);
			recommendationsTable.put("spark.logConf", "default recommendation");
		}
	
		private static void setMaster(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.master", master);
			recommendationsTable.put("spark.master", "default recommendation");
		}
	
		private static void setDriverExtraClassPath(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.extraClassPath", driverExtraClassPath);
			recommendationsTable.put("spark.driver.extraClassPath", "default recommendation");
		}
	
		private static void setDriverExtraJavaOptions(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.extraJavaOptions", driverExtraJavaOptions);
			recommendationsTable.put("spark.driver.extraJavaOptions", "default recommendation");
		}
	
		private static void setDriverExtraLibraryPath(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.extraLibraryPath", driverExtraLibraryPath);
			recommendationsTable.put("spark.driver.extraLibraryPath", "default recommendation");
		}
	
		private static void setDriverUserClassPathFirst(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.userClassPathFirst", driverUserClassPathFirst);
			recommendationsTable.put("spark.driver.userClassPathFirst", "default recommendation");
		}
	
		private static void setExecutorExtraClassPath(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.extraClassPath", executorExtraClassPath);
			recommendationsTable.put("spark.executor.extraClassPath", "default recommendation");
		}
	
		private static void setExecutorExtraJavaOptions(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.extraJavaOptions", executorExtraJavaOptions);
			recommendationsTable.put("spark.executor.extraJavaOptions", "default recommendation");
		}
	
		private static void setExecutorExtraLibraryPath(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.extraLibraryPath", executorExtraLibraryPath);
			recommendationsTable.put("spark.executor.extraLibraryPath", "default recommendation");
		}
	
		private static void setExecutorLogsRollingMaxRetainedFiles(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.logs.rolling.maxRetainedFiles", executorLogsRollingMaxRetainedFiles);
			recommendationsTable.put("spark.executor.logs.rolling.maxRetainedFiles", "default recommendation");
		}
	
		private static void setExecutorLogsRollingMaxSize(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.logs.rolling.maxSize", executorLogsRollingMaxSize);
			recommendationsTable.put("spark.executor.logs.rolling.maxSize", "default recommendation");
		}
	
		private static void setExecutorLogsRollingStrategy(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.logs.rolling.strategy", executorLogsRollingStrategy);
			recommendationsTable.put("spark.executor.logs.rolling.strategy", "default recommendation");
		}
	
		private static void setExecutorLogsRollingTimeInterval(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.logs.rolling.time.interval", executorLogsRollingTimeInterval);
			recommendationsTable.put("spark.executor.logs.rolling.time.interval", "default recommendation");
		}
	
		private static void setExecutorUserClassPathFirst(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.userClassPathFirst", executorUserClassPathFirst);
			recommendationsTable.put("spark.executor.userClassPathFirst", "default recommendation");
		}
		
		private static void setExecutorEnvEnvironmentVariableName(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			
			for (int i = 0; i < executorEnvVariablesArray.size(); i++){
				String optionVariable = "spark.executorEnv." + executorEnvVariablesArray.get(i);
				optionsTable.put(executorEnvValuesArray.get(i), optionVariable);
				recommendationsTable.put(optionVariable, "default recommendation");
			}
			
		}
	
		private static void setPythonProfile(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.python.profile", pythonProfile);
			recommendationsTable.put("spark.python.profile", "default recommendation");
		}
	
		private static void setPythonProfileDump(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.python.profile.dump", pythonProfileDump);
			recommendationsTable.put("spark.python.profile.dump", "default recommendation");
		}
	
		private static void setPythonWorkerMemory(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.python.worker.memory", pythonWorkerMemory);
			recommendationsTable.put("spark.python.worker.memory", "default recommendation");
		}
	
		private static void setPythonWorkerReuse(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.python.worker.reuse", pythonWorkerReuse);
			recommendationsTable.put("spark.python.worker.reuse", "default recommendation");
		}
		
		//Shuffle Behavior
		private static void setReducerMaxSizeInFlight(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.reducer.maxSizeInFlight", reducerMaxSizeInFlight);
			recommendationsTable.put("spark.reducer.maxSizeInFlight", "default recommendation");
		}
	
		private static void setShuffleBlockTransferService(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.blockTransferService", shuffleBlockTransferService);
			recommendationsTable.put("spark.shuffle.blockTransferService", "default recommendation");
		}
	
		private static void setShuffleCompress(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.compress", shuffleCompress);
			recommendationsTable.put("spark.shuffle.compress", "default recommendation");
		}
	
		private static void setConsolidateFiles(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.consolidateFiles", consolidateFiles);
			recommendationsTable.put("spark.shuffle.consolidateFiles", "default recommendation");
		}
	
		private static void setShuffleFileBuffer(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.file.buffer", shuffleFileBuffer);
			recommendationsTable.put("spark.shuffle.file.buffer", "default recommendation");
		}
	
		private static void setShuffleIOMaxRetries(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.io.maxRetries", shuffleIOMaxRetries);
			recommendationsTable.put("spark.shuffle.io.maxRetries", "default recommendation");
		}
	
		private static void setShuffleIONumConnectionsPerPeer(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.io.numConnectionsPerPeer", shuffleIONumConnectionsPerPeer);
			recommendationsTable.put("spark.shuffle.io.numConnectionsPerPeer", "default recommendation");
		}
	
		private static void setShuffleIOPreferDirectBufs(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.io.preferDirectBufs", shuffleIOPreferDirectBufs);
			recommendationsTable.put("spark.shuffle.io.preferDirectBufs", "default recommendation");
		}
	
		private static void setShuffleIORetryWait(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.io.retryWait", shuffleIORetryWait);
			recommendationsTable.put("spark.shuffle.io.retryWait", "default recommendation");
		}
	
		private static void setShuffleManager(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.manager", shuffleManager);
			recommendationsTable.put("spark.shuffle.manager", "default recommendation");
		}
	
		private static void setShuffleMemoryFraction(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.memoryFraction", shuffleMemoryFraction);
			recommendationsTable.put("spark.shuffle.memoryFraction", "default recommendation");
		}
	
		private static void setSortBypassMergeThreshold(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.sort.bypassMergeThreshold", sortBypassMergeThreshold);
			recommendationsTable.put("spark.shuffle.sort.bypassMergeThreshold", "default recommendation");
		}
	
		private static void setShuffleSpill(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.spill", shuffleSpill);
			recommendationsTable.put("spark.shuffle.spill", "default recommendation");
		}
	
		private static void setShuffleSpillCompress(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.shuffle.spill.compress", shuffleSpillCompress);
			recommendationsTable.put("spark.shuffle.spill.compress", "default recommendation");
		}
		
		//Spark UI
		private static void setEventLogCompress(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.eventLog.compress", eventLogCompress);
			recommendationsTable.put("spark.eventLog.compress", "default recommendation");
		}
	
		private static void setEventLogDir(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.eventLog.dir", eventLogDir);
			recommendationsTable.put("spark.eventLog.dir", "default recommendation");
		}
	
		private static void setEventLogEnabled(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.eventLog.enabled", eventLogEnabled);
			recommendationsTable.put("spark.eventLog.enabled", "default recommendation");
		}
	
		private static void setUiKillEnabled(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.ui.killEnabled", uiKillEnabled);
			recommendationsTable.put("spark.ui.killEnabled", "default recommendation");
		}
	
		private static void setUiPort(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.ui.port", uiPort);
			recommendationsTable.put("spark.ui.port", "default recommendation");
		}
	
		private static void setRetainedJobs(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.ui.retainedJobs", retainedJobs);
			recommendationsTable.put("spark.ui.retainedJobs", "default recommendation");
		}
	
		private static void setRetainedStages(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.ui.retainedStages", retainedStages);
			recommendationsTable.put("spark.ui.retainedStages", "default recommendation");
		}
	
		//Compression and Serialization
		private static void setBroadcastCompress(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.broadcast.compress", broadcastCompress);
			recommendationsTable.put("spark.broadcast.compress", "default recommendation");
		}
	
		private static void setClosureSerializer(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.closure.serializer", closureSerializer);
			recommendationsTable.put("spark.closure.serializer", "default recommendation");
		}
	
		private static void setCompressionCodec(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.io.compression.codec", compressionCodec);
			recommendationsTable.put("spark.io.compression.codec", "default recommendation");
		}
	
		private static void setIOCompressionLz4BlockSize(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.io.compression.lz4.blockSize", IOCompressionLz4BlockSize);
			recommendationsTable.put("spark.io.compression.lz4.blockSize", "default recommendation");
		}
	
		private static void setIOCompressionSnappyBlockSize(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.io.compression.snappy.blockSize", IOCompressionSnappyBlockSize);
			recommendationsTable.put("spark.io.compression.snappy.blockSize", "default recommendation");
		}
	
		private static void setKryoClassesToRegister(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryo.classesToRegister", kryoClassesToRegister);
			recommendationsTable.put("spark.kryo.classesToRegister", "default recommendation");
		}
	
		private static void setKryoReferenceTracking(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryo.referenceTracking", kryoReferenceTracking);
			recommendationsTable.put("spark.kryo.referenceTracking", "default recommendation");
		}
	
		private static void setKryoRegistrationRequired(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryo.registrationRequired", registrationRequried);
			recommendationsTable.put("spark.kryo.registrationRequired", "default recommendation");
		}
	
		private static void setKryoRegistrator(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryo.registrator", kryoRegistrator);
			recommendationsTable.put("spark.kryo.registrator", "default recommendation");
		}
	
		private static void setKyroserializerBufferMax(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryoserializer.buffer.max", kyroserializerBufferMax);
			recommendationsTable.put("spark.kryoserializer.buffer.max", "default recommendation");
		}
	
		private static void setKryoserializerBuffer(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.kryoserializer.buffer", kryoserializerBuffer);
			recommendationsTable.put("spark.kryoserializer.buffer", "default recommendation");
		}
	
		private static void setRddCompress(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.rdd.compress", rddCompress);
			recommendationsTable.put("spark.rdd.compress", "default recommendation");
		}
	
		private static void setSerializer(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.serializer", serializer);
			recommendationsTable.put("spark.serializer", "default recommendation");
		}
	
		private static void setSerializerObjectStreamReset(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.serializer.objectStreamReset", serializerObjectStreamReset);
			recommendationsTable.put("spark.serializer.objectStreamReset", "default recommendation");
		}
		
		//Execution Behavior
		private static void setBroadCastBlockSize(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.broadcast.blockSize", broadCastBlockSize);
			recommendationsTable.put("spark.broadcast.blockSize", "default recommendation");
		}
	
		private static void setBroadCastFactory(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.broadcast.factory", broadCastFactory);
			recommendationsTable.put("spark.broadcast.factory", "default recommendation");
		}
	
		private static void setCleanerTtl(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.cleaner.ttl", cleanerTtl);
			recommendationsTable.put("spark.cleaner.ttl", "default recommendation");
		}
	
		private static void setExecutorCores(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.cores", executorCores);
			recommendationsTable.put("spark.executor.cores", "default recommendation");
		}
	
		private static void setDefaultParallelism(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.default.parallelism", defaultParallelism);
			recommendationsTable.put("spark.default.parallelism", "default recommendation");
		}
	
		private static void setExecutorHeartBeatInterval(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.heartbeatInterval", executorHeartBeatInterval);
			recommendationsTable.put("spark.executor.heartbeatInterval", "default recommendation");
		}
	
		private static void setFilesFetchTimeout(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.files.fetchTimeout", filesFetchTimeout);
			recommendationsTable.put("spark.files.fetchTimeout", "default recommendation");
		}
	
		private static void setFilesUseFetchCache(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.files.useFetchCache", filesUseFetchCache);
			recommendationsTable.put("spark.files.useFetchCache", "default recommendation");
		}
	
		private static void setFilesOverwrite(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.files.overwrite", filesOverwrite);
			recommendationsTable.put("spark.files.overwrite", "default recommendation");
		}
	
		private static void setHadoopCloneClonf(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.hadoop.cloneConf", hadoopCloneClonf);
			recommendationsTable.put("spark.hadoop.cloneConf", "default recommendation");
		}
	
		private static void setHadoopValidateOutputSpecs(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.hadoop.validateOutputSpecs", hadoopValidateOutputSpecs);
			recommendationsTable.put("spark.hadoop.validateOutputSpecs", "default recommendation");
		}
	
		private static void setStorageMemoryFraction(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.storage.memoryFraction", storageMemoryFraction);
			recommendationsTable.put("spark.storage.memoryFraction", "default recommendation");
		}
	
		private static void setStorageMemoryMapThreshold(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.storage.memoryMapThreshold", storageMemoryMapThreshold);
			recommendationsTable.put("spark.storage.memoryMapThreshold", "default recommendation");
		}
	
		private static void setStorageUnrollFraction(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.storage.unrollFraction", storageUnrollFraction);
			recommendationsTable.put("spark.storage.unrollFraction", "default recommendation");
		}
	
		private static void setExternalBlockStoreBlockManager(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.externalBlockStore.blockManager", externalBlockStoreBlockManager);
			recommendationsTable.put("spark.externalBlockStore.blockManager", "default recommendation");
		}
	
		private static void setExternalBlockStoreBaseDir(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.externalBlockStore.baseDir", externalBlockStoreBaseDir);
			recommendationsTable.put("spark.externalBlockStore.baseDir", "default recommendation");
		}
	
		private static void setExternalBlockStoreURL(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.externalBlockStore.url", externalBlockStoreURL);
			recommendationsTable.put("spark.externalBlockStore.url", "default recommendation");
		}
		
		//Networking
		private static void setAkkaFailureDetectorThreshold(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.failure-detector.threshold", akkaFailureDetectorThreshold);
			recommendationsTable.put("spark.akka.failure-detector.threshold", "default recommendation");
		}
	
		private static void setAkkaFrameSize(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.frameSize", akkaFrameSize);
			recommendationsTable.put("spark.akka.frameSize", "default recommendation");
		}
	
		private static void setAkkaHeartbeatInterval(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.heartbeat.interval", akkaHeartbeatInterval);
			recommendationsTable.put("spark.akka.heartbeat.interval", "default recommendation");
		}
	
		private static void setAkkaHeartbeatPauses(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.heartbeat.pauses", akkaHeartbeatPauses);
			recommendationsTable.put("spark.akka.heartbeat.pauses", "default recommendation");
		}
	
		private static void setAkkaThreads(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.threads", akkaThreads);
			recommendationsTable.put("spark.akka.threads", "default recommendation");
		}
	
		private static void setAkkaTimeout(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.akka.timeout", akkaTimeout);
			recommendationsTable.put("spark.akka.timeout", "default recommendation");
		}
	
		private static void setBlockManagerPort(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.blockManager.port", blockManagerPort);
			recommendationsTable.put("spark.blockManager.port", "default recommendation");
		}
	
		private static void setBroadcastPort(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.broadcast.port", broadcastPort);
			recommendationsTable.put("spark.broadcast.port", "default recommendation");
		}
	
		private static void setDriverHost(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.host", driverHost);
			recommendationsTable.put("spark.driver.host", "default recommendation");
		}
	
		private static void setDriverPort(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.driver.port", driverPort);
			recommendationsTable.put("spark.driver.port", "default recommendation");
		}
	
		private static void setExecutorPort(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.executor.por", executorPort);
			recommendationsTable.put("spark.executor.por", "default recommendation");
		}
	
		private static void setFileserverPort(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.fileserver.port", fileserverPort);
			recommendationsTable.put("spark.fileserver.port", "default recommendation");
		}
	
		private static void setNetworkTimeout(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.network.timeout", networkTimeout);
			recommendationsTable.put("spark.network.timeout", "default recommendation");
		}
	
		private static void setPortMaxRetries(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.port.maxRetries", portMaxRetries);
			recommendationsTable.put("spark.port.maxRetries", "default recommendation");
		}
	
		private static void setReplClassServerPort(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.replClassServer.port", replClassServerPort);
			recommendationsTable.put("spark.replClassServer.port", "default recommendation");
		}
	
		private static void setRpcNumRetries(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.rpc.numRetries", rpcNumRetries);
			recommendationsTable.put("spark.rpc.numRetries", "default recommendation");
		}
	
		private static void setRpcRetryWait(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.rpc.retry.wait", rpcRetryWait);
			recommendationsTable.put("spark.rpc.retry.wait", "default recommendation");
		}
	
		private static void setRpcAskTimeout(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.rpc.askTimeout", rpcAskTimeout);
			recommendationsTable.put("spark.rpc.askTimeout", "default recommendation");
		}
	
		private static void setRpcLookupTimeout(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.rpc.lookupTimeout", rpcLookupTimeout);
			recommendationsTable.put("spark.rpc.lookupTimeout", "default recommendation");
		}
		
		//Scheduling
		private static void setCoresMax(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.cores.max", coresMax);
			recommendationsTable.put("spark.cores.max", "default recommendation");
		}
	
		private static void setLocalExecutionEnabled(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.localExecution.enabled", localExecutionEnabled);
			recommendationsTable.put("spark.localExecution.enabled", "default recommendation");
		}
	
		private static void setLocalityWait(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.locality.wait", localityWait);
			recommendationsTable.put("spark.locality.wait", "default recommendation");
		}
	
		private static void setLocalityWaitNode(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.locality.wait.node", localityWaitNode);
			recommendationsTable.put("spark.locality.wait.node", "default recommendation");
		}
	
		private static void setLocalityWaitProcess(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.locality.wait.process", localityWaitProcess);
			recommendationsTable.put("spark.locality.wait.process", "default recommendation");
		}
	
		private static void setLocalityWaitRack(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.locality.wait.rack", localityWaitRack);
			recommendationsTable.put("spark.locality.wait.rack", "default recommendation");
		}
	
		private static void setSchedulerMaxRegisteredResourcesWaitingTime(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.scheduler.maxRegisteredResourcesWaitingTime", schedulerMaxRegisteredResourcesWaitingTime);
			recommendationsTable.put("spark.scheduler.maxRegisteredResourcesWaitingTime", "default recommendation");
		}
	
		private static void setSchedulerMinRegisteredResourcesRatio(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.scheduler.minRegisteredResourcesRatio", schedulerMinRegisteredResourcesRatio);
			recommendationsTable.put("spark.scheduler.minRegisteredResourcesRatio", "default recommendation");
		}
	
		private static void setSchedulerMode(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.scheduler.mode", schedulerMode);
			recommendationsTable.put("spark.scheduler.mode", "default recommendation");
		}
	
		private static void setSchedulerReviveInterval(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.scheduler.revive.interval", schedulerReviveInterval);
			recommendationsTable.put("spark.scheduler.revive.interval", "default recommendation");
		}
	
		private static void setSpeculation(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.speculation", speculation);
			recommendationsTable.put("spark.speculation", "default recommendation");
		}
	
		private static void setSpeculationInterval(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.speculation.interval", speculationInterval);
			recommendationsTable.put("spark.speculation.interval", "default recommendation");
		}
	
		private static void setSpeculationMultiplier(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.speculation.multiplier", speculationMultiplier);
			recommendationsTable.put("spark.speculation.multiplier", "default recommendation");
		}
	
		private static void setSpeculationQuantile(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.speculation.quantile", speculationQuantile);
			recommendationsTable.put("spark.speculation.quantile", "default recommendation");
		}
	
		private static void setTaskCpus(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.task.cpus", taskCpus);
			recommendationsTable.put("spark.task.cpus", "default recommendation");
		}
	
		private static void setTaskMaxFailures(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
			optionsTable.put("spark.task.maxFailures", taskMaxFailures);
			recommendationsTable.put("spark.task.maxFailures", "default recommendation");
		}

		public static void setNetworking(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			setAkkaFailureDetectorThreshold(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setAkkaFrameSize(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setAkkaHeartbeatInterval(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setAkkaHeartbeatPauses(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setAkkaThreads(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setAkkaTimeout(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setBlockManagerPort(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setBroadcastPort(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setDriverHost(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setDriverPort(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorPort(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setFileserverPort(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setNetworkTimeout(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setPortMaxRetries(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setReplClassServerPort(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setRpcNumRetries(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setRpcRetryWait(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setRpcAskTimeout(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setRpcLookupTimeout(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setCoresMax(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setLocalExecutionEnabled(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setLocalityWait(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setLocalityWaitNode(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setLocalityWaitProcess(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setLocalityWaitRack(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSchedulerMaxRegisteredResourcesWaitingTime(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSchedulerMinRegisteredResourcesRatio(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSchedulerMode(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSchedulerReviveInterval(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSpeculation(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSpeculationInterval(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSpeculationMultiplier(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSpeculationQuantile(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setTaskCpus(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setTaskMaxFailures(args, optionsTable, recommendationsTable, commandLineParamsTable);
		}

		public static void setExecutionBehavior(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			setBroadCastBlockSize(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setBroadCastFactory(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setCleanerTtl(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorCores(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setDefaultParallelism(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorHeartBeatInterval(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setFilesFetchTimeout(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setFilesUseFetchCache(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setFilesOverwrite(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setHadoopCloneClonf(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setHadoopValidateOutputSpecs(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setStorageMemoryFraction(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setStorageMemoryMapThreshold(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setStorageUnrollFraction(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExternalBlockStoreBlockManager(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExternalBlockStoreBaseDir(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExternalBlockStoreURL(args, optionsTable, recommendationsTable, commandLineParamsTable);
		}

		public static void setCompressionSerialization(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			setBroadcastCompress(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setClosureSerializer(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setCompressionCodec(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setIOCompressionLz4BlockSize(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setIOCompressionSnappyBlockSize(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setKryoClassesToRegister(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setKryoReferenceTracking(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setKryoRegistrationRequired(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setKryoRegistrator(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setKyroserializerBufferMax(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setKryoserializerBuffer(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setRddCompress(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSerializer(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSerializerObjectStreamReset(args, optionsTable, recommendationsTable, commandLineParamsTable);
		}

		public static void setSparkUI(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			setEventLogCompress(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setEventLogDir(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setEventLogEnabled(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setUiKillEnabled(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setUiPort(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setRetainedJobs(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setRetainedStages(args, optionsTable, recommendationsTable, commandLineParamsTable);
		}

		public static void setShuffleBehavior(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			setReducerMaxSizeInFlight(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleBlockTransferService(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleCompress(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setConsolidateFiles(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleFileBuffer(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleIOMaxRetries(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleIONumConnectionsPerPeer(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleIOPreferDirectBufs(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleIORetryWait(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleManager(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleMemoryFraction(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setSortBypassMergeThreshold(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleSpill(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setShuffleSpillCompress(args, optionsTable, recommendationsTable, commandLineParamsTable);
		}

		public static void setRunTimeEnvironment(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			setDriverExtraClassPath(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setDriverExtraJavaOptions(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setDriverExtraLibraryPath(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setDriverUserClassPathFirst(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorExtraClassPath(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorExtraJavaOptions(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorExtraLibraryPath(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorLogsRollingMaxRetainedFiles(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorLogsRollingMaxSize(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorLogsRollingStrategy(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorLogsRollingTimeInterval(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorUserClassPathFirst(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorEnvEnvironmentVariableName(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setPythonProfile(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setPythonProfileDump(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setPythonWorkerMemory(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setPythonWorkerReuse(args, optionsTable, recommendationsTable, commandLineParamsTable);
		}

		public static void setApplicationProperties(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
			setAppName(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setDriverCores(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setMaxResultSize(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setDriverMemory(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExecutorMemory(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setExtraListeners(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setLocalDir(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setLogConf(args, optionsTable, recommendationsTable, commandLineParamsTable);
		    setMaster(args, optionsTable, recommendationsTable, commandLineParamsTable);
		}

		public static void configureStandardSettings(
				String[] args, Hashtable<String, String> optionsTable,
				Hashtable<String, String> recommendationsTable,
				Hashtable<String, String> commandLineParamsTable) {

			// Set Application Properties
			setApplicationProperties(args, optionsTable, recommendationsTable, commandLineParamsTable);
			// Set RunTime Environment
			setRunTimeEnvironment(args, optionsTable, recommendationsTable, commandLineParamsTable);
			// Set Shuffle Behavior
			setShuffleBehavior(args, optionsTable, recommendationsTable, commandLineParamsTable);
			// Set Spark UI
			setSparkUI(args, optionsTable, recommendationsTable, commandLineParamsTable);
			// Set Compression and Serialization
			setCompressionSerialization(args, optionsTable, recommendationsTable, commandLineParamsTable);
			// Set Execution Behavior
			setExecutionBehavior(args, optionsTable, recommendationsTable, commandLineParamsTable);
			// Set Networking
			setNetworking(args, optionsTable, recommendationsTable, commandLineParamsTable);

		}
	}
