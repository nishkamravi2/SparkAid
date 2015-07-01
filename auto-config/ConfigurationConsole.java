import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Arrays;

import security.src.main.Security;
import sparkR.src.main.SparkR;
import sql.src.main.SQL;
import streaming.src.main.Streaming;
import core.src.main.standalone.Standalone;
import core.src.main.yarn.Yarn;
import dynamicallocation.src.main.DynamicAllocation;


public class ConfigurationConsole {
	
	public static void main(String[] args) {
		
		/** 
		 * Simple tool to initialize and configure Spark config params, generate the command line and advise
		 * @author Nishkam Ravi (nravi@cloudera.com), Ethan Chan (yish.chan@gmail.com)
		 */
		
		printUsage();
		System.out.println("Input Args: " + Arrays.toString(args));

		// input config parameters
		String inputDataSize = ""; // in GB
		String numNodes = "";
		String numCoresPerNode = "";
		String memoryPerNode = ""; // in GB
		String numJobs = "";
		String fileSystem = ""; // ext4, ext3, etc
		String master = ""; // standalone, yarn
		String deployMode = ""; // client, cluster
		String clusterManager = ""; // standalone, yarn
		String sqlFlag = ""; // y, n
		String streamingFlag = "";// y, n
		String dynamicAllocationFlag = "";//y, n
		String securityFlag = "";// y, n
		String sparkRFlag = "";// y, n
		String className = ""; // name of app class
		String appJar = ""; // app Jar URL
		String appArgs = ""; // app args as a single string
		
		//input table
		Hashtable<String, String> inputsTable = new Hashtable<String, String>();
		
		//output tables
		Hashtable<String, String> optionsTable = new Hashtable<String, String>();
		Hashtable<String, String> recommendationsTable = new Hashtable<String, String>();
		Hashtable<String, String> commandLineParamsTable = new Hashtable<String, String>();
		
		String cmdLineParams = "";
		
		// get input parameters
		if (args.length != 17) {
			System.out.println("Invalid Input\n");
			printUsage();
			System.exit(0);
		} else {
			inputDataSize = args[0];
			numNodes = args[1];
			numCoresPerNode = args[2];
			//for yarn it is container memory, for standalone will be node memory
			memoryPerNode = args[3];
			numJobs = args[4];
			fileSystem = args[5];
			master = args[6];
			deployMode = args[7];
			clusterManager = args[8];
			sqlFlag = args[9];
			streamingFlag = args[10];
			dynamicAllocationFlag = args[11];
			securityFlag = args[12];
			sparkRFlag = args[13];
			className = args[14];
			appJar = args[15];
			appArgs = args[16];
			
			inputsTable.put("inputDataSize", inputDataSize);
			inputsTable.put("numNodes", numNodes);
			inputsTable.put("numCoresPerNode", numCoresPerNode);
			inputsTable.put("memoryPerNode", memoryPerNode);
			inputsTable.put("numJobs", numJobs);			
			inputsTable.put("fileSystem", fileSystem);
			inputsTable.put("master", master);
			inputsTable.put("deployMode", deployMode);
			inputsTable.put("clusterManager", clusterManager);
			inputsTable.put("sqlFlag", sqlFlag);
			inputsTable.put("streamingFlag", streamingFlag);
			inputsTable.put("dynamicAllocationFlag", dynamicAllocationFlag);
			inputsTable.put("securityFlag", securityFlag);
			inputsTable.put("sparkRFlag", sparkRFlag);
			inputsTable.put("className", className);
			inputsTable.put("appJar", appJar);
			inputsTable.put("appArgs", appArgs);
		}
		
		//first initialize standard/standalone parameters
		Standalone.configureStandardSettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		
		//if it is yarn, add in the Yarn settings
		if (clusterManager.equals("yarn")){ Yarn.configureYarnSettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);}
		
		//configure necessary Dynamic Allocation settings
		if (dynamicAllocationFlag.equals("y")){ DynamicAllocation.configureDynamicAllocationSettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);}
		
		//configure necessary SQL settings
		if (sqlFlag.equals("y")){ SQL.configureSQLSettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable); }
		
		//configure necessary Streaming settings
		if (streamingFlag.equals("y")){ Streaming.configureStreamingSettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);}
		
		//configure necessary Security settings
		if (securityFlag.equals("y")){ Security.configureSecuritySettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);}
		
		//configure necessary SparkR settings
		if (sparkRFlag.equals("y")){ SparkR.configureSparkRSettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);}

		try {
			//Creating the .conf file
			File conf_file = new File("spark.conf");
			if (!conf_file.exists()) {
				conf_file.createNewFile();
			}
			//Creating the recommendations file
			BufferedWriter b1 = new BufferedWriter(
					new FileWriter(conf_file));

			File advise_file = new File("spark.conf.advise");
			if (!advise_file.exists()) {
				advise_file.createNewFile();
			}
			
			//Creating the command line options file
			BufferedWriter b2 = new BufferedWriter(new FileWriter(
					advise_file));

			File cmd_options_file = new File("spark.cmdline.options");
			if (!cmd_options_file.exists()) {
				cmd_options_file.createNewFile();
			}
			BufferedWriter b3 = new BufferedWriter(new FileWriter(
					cmd_options_file));

			//Populating Options
			Enumeration<String> it = optionsTable.keys();
			while (it.hasMoreElements()) {
				String key = it.nextElement();
				String value = optionsTable.get(key);
				// if nothing was set, do not add it to .conf file
				if (value.equals("")) {
					continue;
				}
				b1.write(key + " 			" + value + "\n");
			}
			b1.close();

			//Populating Recommendations
			it = recommendationsTable.keys();
			while (it.hasMoreElements()) {
				String key = it.nextElement();
				String value = recommendationsTable.get(key);
				// if nothing was set, do not add it to .conf file
				if (value.equals("")) {
					continue;
				}
				b2.write(key + " 			" + value + "\n");
			}
			b2.close();

			//Populating Command Line Params
			it = commandLineParamsTable.keys();
			while (it.hasMoreElements()) {
				String key = it.nextElement();
				String value = commandLineParamsTable.get(key);
				// if nothing was set, do not add it to command line params
				if (value.equals("")) {
					continue;
				}
				b3.write(key + " 			" + value + "\n");
				cmdLineParams += " " + key + " " + value;
			}
			b3.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		constructCmdLine(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable, cmdLineParams);
	}
	public static void printUsage() {
		System.out.println("Usage: \n"
				+ "./run.sh \n"
				+ "<input data size in GB> \n"
				+ "<number of nodes in cluster> \n"
				+ "<number of cores per node> \n"
				+ "<memory per node in GB> \n"
				+ "<number of jobs> \n"
				+ "<filesystem type> \n"
				+ "<master: standalone URL/yarn> \n"
				+ "<deployMode: cluster/client> \n"
				+ "<clusterManger: standalone/yarn> \n"
				+ "<sql: y/n> \n"
				+ "<streaming: y/n> \n"
				+ "<dynamicAllocation: y/n> \n"
				+ "<security: y/n> \n"
				+ "<sparkR: y/n> \n"
				+ "<app className> \n"
				+ "<app JAR location> \n"
				+ "<app arguments as one string>\n");
	}

	public static void constructCmdLine(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable, String cmdLineParams){
		String master = inputsTable.get("master");
		String deployMode = inputsTable.get("deployMode");
		String className = inputsTable.get("className");
		String appJar = inputsTable.get("appJar");
		String appArgs = inputsTable.get("appArgs");
		String cmdLine = "spark-submit --master " + master 
				+ " --deploy-mode " + deployMode 
				+ " --class " + className 
				+ " --properties-file spark.conf " 
				+ cmdLineParams + " " + appJar + " " + appArgs;
		
		System.out.println("\nAuto-generated files: spark.conf, spark.conf.advise and spark.cmdline.options\n");
		System.out.println("Invoke command line: " + cmdLine + "\n");
	}
}
