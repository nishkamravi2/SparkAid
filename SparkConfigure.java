/** 
 * Simple tool to initialize and configure Spark config params, generate the command line and advise
 * @author Nishkam Ravi (nravi@cloudera.com)
 */

import java.util.*;
import java.io.*;

public class SparkConfigure{

	//input config parameters
	static String inputDataSize; //in GB
	static String numNodes; 
	static String numCoresPerNode;
     	static String memoryPerNode; //in GB
        static String numJobs;
	static String fileSystem; //ext4, ext3, etc 
        static String master; //standalone, yarn
	static String deployMode; //client, cluster
	static String className; //name of app class
	static String appJar; //app Jar URL
	static String appArgs; //app args as a single string

	//default config params
	static String storageMemoryFraction = "0.6";
	static String shuffleMemoryFraction = "0.3";
        static String executorMemory = "0.5"; //512 MB
	static String driverMemory = "0.5"; //512 MB
	static String executorInstances = "2"; 
	static String executorCores = "1"; 
	static String sparkSerializer = "org.apache.spark.serializer.JavaSerializer";
	static String coresMax = ""; //all cores in the cluster
	static String schedulerMode = "FIFO";
	static String workerTimeout = "60"; //seconds
	static String akkaFramesize = "10";
	static String akkaThreads = "4";
	static String akkaTimeout = "100"; //seconds
	static String akkaHeartbeatPauses = "600"; //seconds
	static String akkaFailureDetectorThreshold = "300"; //seconds 
	static String akkaHeartbeatInterval = "1000"; //seconds
	static String shuffleConsolidateFiles ="false";
	static String streamingUnpersist = "false";
        static String streamingBlockInterval = "200"; //seconds
	static String shuffleFileBufferKb = "100"; //KB
	static String shuffleSpill = "true"; 
	static String speculation = "false"; 
	static String speculationInterval = "100"; //milliseconds
	static String speculationQuantile = "0.75"; 
	static String speculationMultiplier = "1.5";
	static String deploySpreadout = "true";
	static String defaultParallelism = "8";
	static String shuffleCompress = "true";
	static String spillCompress = "true";
	static String broadcastCompress = "true";
	static String rddCompress = "false";
	static String compressionCodec = "org.apache.spark.io.LZFCompressionCodec";
	static String snappyBlockSize = "32768"; //bytes
	static String reducerMaxMbInFlight = "48"; //MB
	static String kryoserializerBufferMb = "2"; //MB
	static String broadcastFactory = "org.apache.spark.broadcast.HttpBroadcastFactory";
	static String localityWait = "3000"; //milliseconds
	static String localDir = "/tmp";
	static String maxFailures = "4";
	static String broadcastBlockSize = "4096";
	static String blockManagerSlaveTimeoutMs = "45000"; //milliseconds
	static String yarnExecutorMemoryOverhead = "384"; //MB

	static String extraJavaOptions = ""; //gcOptions and such 
 	static String storageLevel = "";
	static String driverCores = "1";

        static Hashtable ht1; //for storing params that will be used in constructing spark options
	static Hashtable ht2; //for storing recommendations to the developer
        static Hashtable ht3; //for storing other command-line params

	static String cmdLineParams = "";

	public static void setStorageMemoryFraction(){
		ht1.put("spark.storage.memoryFraction", storageMemoryFraction);
		ht2.put("spark.storage.memoryFraction", "reduce this value down to 0.1 if there is no RDD caching/persistence in the app");
	}

	public static void setShuffleMemoryFraction(){
		ht1.put("spark.shuffle.memoryFraction", shuffleMemoryFraction);
		ht2.put("spark.shuffle.memoryFraction", "increase this value to 0.8 if there is no RDD caching/persistence in the app");
	}

        public static void setExecutorMemory(){
		double expectedMemoryReq = Double.parseDouble(inputDataSize)*5.0;
                double memory = Double.parseDouble(memoryPerNode);
                double fraction = 1.0;
		if(memory < 1.0){
			fraction = 0.7;
		}else if(memory < 10.0){
			fraction = 0.75;
		}else if(memory < 50.0){
			fraction = 0.85;
		}else if(memory < 100.0){
			fraction = 0.90;
		}else if(memory < 500.0){
			fraction = 0.95;
		}else{
			fraction = 0.98;
		}
		if(master.equals("yarn")){
			fraction = fraction-0.05;
		}
		double totalAvailableMemory = fraction*memory/Double.parseDouble(numJobs);
		executorMemory = (int)totalAvailableMemory + "";
		ht1.put("spark.executor.memory", executorMemory + "g");
	}

	public static void setSparkSerializer(){
		if(storageLevel.contains("SER")){
			sparkSerializer = "KryoSerializer";
		}
		ht2.put("spark.serializer", sparkSerializer);
	}

	public static void setCoresMax(){
		int cores = Integer.parseInt(numCoresPerNode)*Integer.parseInt(numNodes)/Integer.parseInt(numJobs);
                coresMax = cores + "";
		ht1.put("spark.cores.max", coresMax);
	}

	public static void setSchedulerMode(){
		if(Double.parseDouble(numJobs) > 1){
			schedulerMode = "FAIR";
		}
		ht1.put("spark.scheduler.mode", schedulerMode);
	}

	public static void setWorkerTimeout(){
		ht1.put("spark.worker.timeout", workerTimeout);
		ht2.put("spark.worker.timeout", "increase if GC pauses cause problem");
	
	}

	public static void setAkkaFramesize(){
		ht1.put("spark.akka.framesize", akkaFramesize);
	}

	public static void setAkkaThreads(){
		ht1.put("spark.akka.threads", akkaThreads);
	}

	public static void setAkkaTimeout(){
		ht1.put("spark.akka.timeout", akkaTimeout);
		ht2.put("spark.akka.timeout", "increase if GC pauses cause problem");
	}

	public static void setAkkaHeartbeatPauses(){
		ht1.put("spark.akka.heartbeat.pauses", akkaHeartbeatPauses);
	}

	public static void setAkkaFailureDetectorThreshold(){
		ht1.put("spark.akka.failure-detector.threshold", akkaFailureDetectorThreshold);
	}

	public static void setAkkaHeartbeatInterval(){
		ht1.put("spark.akka.heartbeat.interval", akkaHeartbeatInterval);
	}

	public static void setShuffleConsolidateFiles(){
		if(fileSystem.equals("ext4") || fileSystem.equals("xfs")){
			ht1.put("spark.shuffle.consolidateFiles", "true");
		}else{
			ht1.put("spark.shuffle.consolidateFiles", shuffleConsolidateFiles);
		}
	}

	public static void setStreamingUnpersist(){
		ht1.put("spark.streaming.unpersist", streamingUnpersist);
		ht2.put("spark.streaming.unpersist", "set this to true if running streaming app and running into OOM issues");
	}

        public static void setStreamingBlockInterval(){
		ht1.put("spark.streaming.blockInternal", streamingBlockInterval);
	}

	public static void setShuffleFileBufferKb(){
		ht1.put("spark.shuffle.file.buffer.kb", shuffleFileBufferKb);
		ht2.put("spark.shuffle.file.buffer.kb", "increase this value to improve shuffle performance for large datasets when lot of memory available");
	}

	public static void setShuffleSpill(){
		ht1.put("spark.shuffle.spill", shuffleSpill);
	}

	public static void setSpeculation(){
		ht1.put("spark.speculation", speculation);
		ht2.put("spark.speculation", "set to true if straggler tasks found"); 
	}

	public static void setSpeculationInterval(){
		ht1.put("spark.speculation.interval", speculationInterval);
	}

	public static void setSpeculationQuantile(){
		ht1.put("spark.speculation.quantile", speculationQuantile);
	}

	public static void setSpeculationMultiplier(){
		ht1.put("spark.speculation.multiplier", speculationMultiplier);
	}

	public static void setDeploySpreadout(){
		ht1.put("spark.deploy.spreadout", deploySpreadout);
	}

	public static void setDefaultParallelism(){
		int parallelism = Integer.parseInt(numCoresPerNode)*Integer.parseInt(numNodes)*2;
		defaultParallelism = parallelism + "";
		ht1.put("spark.default.parallelism", defaultParallelism);
	}

	public static void setShuffleCompress(){
		ht1.put("spark.shuffle.compress", shuffleCompress);
	}

	public static void setSpillCompress(){
		ht1.put("spark.shuffle.spill.compress", spillCompress);
	}

	public static void setBroadcastCompress(){
		ht1.put("spark.broadcast.compress", broadcastCompress);
	}

	public static void setRddCompress(){
		ht1.put("spark.rdd.compress", rddCompress);
		double expectedMemoryReq = Double.parseDouble(inputDataSize)*5.0;
		double ratio = Double.parseDouble(executorMemory)*Float.parseFloat(storageMemoryFraction)*Float.parseFloat(numNodes)/expectedMemoryReq;
		if(ratio <= 1){
			ht2.put("spark.rdd.compress", "set to true to save space at the cost of extra CPU time");
		}
	}

	public static void setCompressionCodec(){
		ht1.put("spark.io.compression.codec", compressionCodec);
		ht2.put("spark.io.compression.codec", "can also try org.apache.spark.io.SnappyCompressionCodec");
	}

	public static void setSnappyBlockSize(){
		ht1.put("spark.io.compression.snappy.block.size", snappyBlockSize); 
	}

	public static void setReducerMaxMbInFlight(){
		ht1.put("spark.reducer.maxMbInFlight", reducerMaxMbInFlight);
		ht2.put("spark.reducer.maxMbInFlight", "increase this value if lot of memory available");	
	}

	public static void setKryoserializerBufferMb(){
		ht1.put("spark.kryoserializer.buffer.mb", kryoserializerBufferMb);
		ht2.put("spark.kryoserializer.buffer.mb", "set size to largest object to be serialized");
	}

	public static void setBroadcastFactory(){
		ht1.put("spark.broadcast.factory", broadcastFactory);
	}

	public static void setLocalityWait(){
		ht1.put("spark.locality.wait", localityWait);
		ht2.put("spark.locality.wait", "increase this value if long GC pauses"); 
	}

	public static void setLocalDir(){
		ht1.put("spark.local.dir", localDir);
		ht2.put("spark.local.dir", "set this to a location with TBs of space");
	}

	public static void setMaxFailures(){
		ht1.put("spark.task.maxFailures", maxFailures);
	}

	public static void setBroadcastBlockSize(){
		ht1.put("spark.broadcast.blockSize", broadcastBlockSize);
	}

	public static void setBlockManagerSlaveTimeoutMs(){
		ht1.put("spark.storage.blockManagerSlaveTimeoutMs", blockManagerSlaveTimeoutMs);
		ht2.put("spark.storage.blockManagerSlaveTimeoutMs", "increase value if long GC pauses");
	}

 	public static void setStorageLevel(){
		double expectedMemoryReq = Double.parseDouble(inputDataSize)*5.0;
		double ratio = Double.parseDouble(executorMemory)*Float.parseFloat(storageMemoryFraction)*Float.parseFloat(numNodes)/expectedMemoryReq;
		if(ratio > 3){
			storageLevel = "MEMORY_ONLY";
		}else if(ratio > 2){
			storageLevel = "MEMORY_AND_DISK";
		}else{
			storageLevel = "MEMORY_AND_DISK_SER";
		}
		ht2.put("spark.storage.level", storageLevel);

	}

	public static void setYarnExecutorMemoryOverhead(){
		double memory = Double.parseDouble(executorMemory);
		yarnExecutorMemoryOverhead = (int)((memory*0.07)*1024) + "" ; //in MB
		ht1.put("spark.yarn.executor.memoryOverhead", yarnExecutorMemoryOverhead);
	}

	public static void setExtraJavaOptions(){
		ht1.put("spark.driver.extraJavaOptions",  "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps");
		ht1.put("spark.executor.extraJavaOptions",  "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps");
		ht2.put("gcOptions", "in case of long gc pauses, try adding the following: -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled");
	}

	public static void setDriverMemory(){
		ht1.put("spark.driver.memory", executorMemory + "g");
		ht3.put("--driver-memory", executorMemory + "g");
	}

	public static void setExecutorInstances(){
		ht1.put("spark.executor.instances", (Integer.parseInt(numNodes)-1) + "");
		ht3.put("--num-executors", (Integer.parseInt(numNodes)-1) + "");
	}

	public static void setExecutorCores(){
		ht1.put("spark.executor.cores", numCoresPerNode); 
	}
	public static void setDriverCores(){
		ht3.put("--driver-cores", numCoresPerNode);
	}

	public static void constructCmdLine(){
		String cmdLine = "spark-submit --master " + master + " --deploy-mode " + deployMode + " --class " + className + " --properties-file spark.conf " + cmdLineParams + " " + 
			appJar + " " + appArgs;
		System.out.println();
		System.out.println("Auto-generated files: spark.conf, spark.conf.advise and spark.cmdline.options");
		System.out.println();
		System.out.println("Invoke command line: " + cmdLine + "\n");
	}

	public static void printUsage(){
		System.out.println();
		System.out.println("Usage: \njava SparkConfigure <input data size in GB> <number of nodes in cluster> <number of cores per node> <memory per node in GB> <number of jobs> <filesystem type> <master: standalone URL/yarn> <deployMode: cluster/client> <app className> <app JAR location> <app arguments as one string>");
		System.out.println();
	}

	public static void main(String args[]){

		//get input parameters
		if(args.length != 11){
			printUsage();
			System.exit(0);
		}else{
			inputDataSize = args[0];
			numNodes = args[1];
			numCoresPerNode = args[2];
			memoryPerNode = args[3];
			numJobs = args[4];
			fileSystem = args[5];
			master = args[6];
			deployMode = args[7];
			className = args[8];
			appJar = args[9];
			appArgs = args[10];
		}

		//create hashtables for storage
		ht1 = new Hashtable<String, String>();
		ht2 = new Hashtable<String, String>();
		ht3 = new Hashtable<String, String>();

		//set important Spark parameters
		setStorageMemoryFraction();
		setShuffleMemoryFraction();
		setExecutorMemory();
		setStorageLevel();
		setSparkSerializer();
		setCoresMax();
		setSchedulerMode();
		setWorkerTimeout();
		setAkkaFramesize();
		setAkkaThreads();
		setAkkaTimeout();
		setAkkaHeartbeatPauses();
		setAkkaFailureDetectorThreshold();
		setAkkaHeartbeatInterval();
		setShuffleConsolidateFiles();
		setStreamingUnpersist();
		setStreamingBlockInterval();
		setShuffleFileBufferKb();
		setShuffleSpill();
		setSpeculation();
		setSpeculationInterval();
		setSpeculationQuantile();
		setSpeculationMultiplier();
		setDeploySpreadout();
		setDefaultParallelism();
		setShuffleCompress();
		setSpillCompress();
		setBroadcastCompress();
		setRddCompress();
		setCompressionCodec();
		setSnappyBlockSize();
		setReducerMaxMbInFlight();
		setKryoserializerBufferMb();
		setBroadcastFactory();
		setLocalityWait();
		setLocalDir();
		setMaxFailures();
		setBroadcastBlockSize();
		setBlockManagerSlaveTimeoutMs();
		setYarnExecutorMemoryOverhead();
		setDriverMemory();
		setExecutorInstances();
		setExecutorCores();
		setDriverCores();
		setExtraJavaOptions();

		//populate files
		try{
			File conf_file = new File("spark.conf");
			if (!conf_file.exists()) {
				conf_file.createNewFile();
			}
			BufferedWriter b1 = new BufferedWriter(new FileWriter(conf_file));

			File advise_file = new File("spark.conf.advise");
			if(!advise_file.exists()) {
				advise_file.createNewFile();
			}
			BufferedWriter b2 = new BufferedWriter(new FileWriter(advise_file));	

			File cmd_options_file = new File("spark.cmdline.options");
			if(!cmd_options_file.exists()) {
				cmd_options_file.createNewFile();
			}
			BufferedWriter b3 = new BufferedWriter(new FileWriter(cmd_options_file));

			Enumeration it = ht1.keys();
			while(it.hasMoreElements()){
				String key = (String) it.nextElement();
				String value = (String) ht1.get(key);
				b1.write(key + " 			" + value + "\n");
			}
			b1.close();

			it = ht2.keys();
			while(it.hasMoreElements()){
				String key = (String) it.nextElement();
				String value = (String) ht2.get(key);
				b2.write(key + " 			" + value + "\n");
			}
			b2.close();

			it = ht3.keys();
			while(it.hasMoreElements()){
				String key = (String) it.nextElement();
				String value = (String ) ht3.get(key);
				b3.write(key + " 			" + value + "\n");
				cmdLineParams += " " + key + " " + value;
			}
			b3.close();
		} catch (Exception e){e.printStackTrace();}

		//construct command line
		constructCmdLine();
	}
}

