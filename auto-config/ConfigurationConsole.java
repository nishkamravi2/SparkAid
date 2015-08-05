import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;

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
		
//		printUsage();
//		System.out.println("Input Args: " + Arrays.toString(args));

		// input config parameters
		String inputDataSize = ""; // in GB
		String numNodes = "";
		String numCoresPerNode = "";
		String memoryPerNode = ""; // in GB
		String resourceFraction = ""; //0 - 1.0 for now
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

		//get input parameters
		if (args.length == 0){
			Scanner scanner = new Scanner(System.in);
			
			System.out.print("Enter input data size in GB:\n");
			inputDataSize = scanner.nextLine();
			
			System.out.println("Enter number of nodes in cluster (including master):");
			numNodes = scanner.nextLine();
			
			System.out.println("Enter number of cores per node: ");
			numCoresPerNode = scanner.nextLine();
			
			System.out.println("Enter memory per node in GB: ");
			memoryPerNode = scanner.nextLine();
			
			System.out.println("Enter fraction of resources of cluster to be used (from 0.0 - 1.00): ");
			resourceFraction = scanner.nextLine();
			
			System.out.println("Enter file system of input raw data: hdfs/ext4/xfs");
			fileSystem = scanner.nextLine();
			
			System.out.println("Enter master URL");
			master = scanner.nextLine();
			
			System.out.println("Enter deploy mode: cluster / client");
			deployMode = scanner.nextLine();
			
			System.out.println("Enter Cluster Manager: standalone / yarn");
			clusterManager = scanner.nextLine();
			
			System.out.println("Is this a SQL application? y/n");
			sqlFlag = scanner.nextLine();
			
			System.out.println("Is this a Streaming application? y/n");
			streamingFlag = scanner.nextLine();
			
			System.out.println("Is this a Dynamic Allocation application? y/n");
			dynamicAllocationFlag = scanner.nextLine();
			
			System.out.println("Is this a Security application? y/n");
			securityFlag = scanner.nextLine();
			
			System.out.println("Is this a sparkR application? y/n");
			sparkRFlag = scanner.nextLine();
			
			System.out.println("Enter Class Name of application: ");
			className = scanner.nextLine();
			
			System.out.println("Enter file path of application JAR ");
			appJar = scanner.nextLine();
			
			System.out.println("Enter Application Arguments");
			appArgs = scanner.nextLine();
		}
		else if(args.length != 17) {
			System.out.println("Invalid Input\n");
			printUsage();
			System.exit(0);
		} else {
			inputDataSize = args[0];
			numNodes = args[1];
			numCoresPerNode = args[2];
			//for yarn it is container memory, for standalone will be node memory
			memoryPerNode = args[3];
			resourceFraction = args[4];
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
		
		}
		
		inputsTable.put("inputDataSize", inputDataSize);
		inputsTable.put("numNodes", numNodes);
		inputsTable.put("numCoresPerNode", numCoresPerNode);
		inputsTable.put("memoryPerNode", memoryPerNode);
		inputsTable.put("resourceFraction", resourceFraction);			
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
		
		//First populates options from default configuration file
        String fileName = "spark.master.default.conf";
        String line = null;

        try {
            FileReader fileReader = new FileReader(fileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while((line = bufferedReader.readLine()) != null) {
            	if (line.length() == 0 || !line.substring(0, 5).equals("spark")){
            		continue;
            	}
                String [] lineArray = line.split("\\s+");
                String optionKey = lineArray[0];
                String optionValue = lineArray[1];
                optionsTable.put(optionKey, optionValue);
            }    
            bufferedReader.close();            
        }
        catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + "'");                
        }
        catch(IOException ex) {
            System.out.println("Error reading file '" + fileName + "'");                   
        }
        
		//first initialize standard/standalone parameters
		Standalone.configureStandardSettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		
		//if it is yarn, add in the Yarn settings
		if (clusterManager.equals("yarn")){ Yarn.configureYarnSettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);}
		
		//configure necessary Dynamic Allocation settings
		if (dynamicAllocationFlag.equals("y")){ 
			if (clusterManager.equals("yarn")){
				DynamicAllocation.configureDynamicAllocationSettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
			}
			else{
				System.out.println("Dynamic Allocation currently only allowed in Yarn mode - Spark 1.5.0, re-enter parameters");
				System.exit(0);
			}
			
		}
		
		//configure necessary SQL settings
		if (sqlFlag.equals("y")){ SQL.configureSQLSettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable); }
		
		//configure necessary Streaming settings
		if (streamingFlag.equals("y")){ Streaming.configureStreamingSettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);}
		
		//configure necessary Security settings
		if (securityFlag.equals("y")){ Security.configureSecuritySettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);}
		
		//configure necessary SparkR settings
		if (sparkRFlag.equals("y")){ SparkR.configureSparkRSettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);}
		
		createOutputFile("spark.conf", optionsTable, "options");
		createOutputFile("spark.conf.advise", recommendationsTable, "recommendations");
		createOutputFile("spark.cmdline.options", commandLineParamsTable, "commandline");
		String cmdLineParams = generateParamsString(commandLineParamsTable);
		constructCmdLine(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable, cmdLineParams);
	}
	
	private static String generateParamsString(Hashtable<String,String> t) {
		
		String cmdLineParams = "";
		ArrayList <String> tableKeySet = new ArrayList<String>(t.keySet());
		Collections.sort(tableKeySet);
		Iterator <String> it = tableKeySet.iterator();

		while (it.hasNext()) {
			String key = it.next();
			String value = t.get(key);
			// if nothing was set, do not add it to output
			if (value.equals("")) {
				continue;
			}
			cmdLineParams += " " + key + " " + value;
		}
		return cmdLineParams;
	}
	
	private static String spaceBuffer (int n){
		StringBuffer outputBuffer = new StringBuffer(n);
		for (int i = 0; i < n; i++){
		   outputBuffer.append(" ");
		}
		return outputBuffer.toString();
	}

	private static Hashtable <String, String> extractKeyCategory(String category, Hashtable<String, String> table, int startingIndex){
		ArrayList <String> tableKeySet = new ArrayList<String>(table.keySet());
		Iterator <String> it = tableKeySet.iterator();
		
		Hashtable<String,String> categoryTable = new Hashtable<String, String>();

		while (it.hasNext()) {
			String key = it.next();
			String value = table.get(key);
			if (key.length() >= startingIndex + category.length() && key.substring(startingIndex, startingIndex + category.length()).equals(category)){
				categoryTable.put(key, value); //put it in the new list
				table.remove(key); //removes it from the original table
			}
		}
		return categoryTable;
		
	}
	
	private static void writeCategory(Hashtable <String, String> catTable , BufferedWriter b){
		int startParamIndex = 70;
		ArrayList <String> tableKeySet = new ArrayList<String>(catTable.keySet());
		Collections.sort(tableKeySet);
		Iterator <String> it = tableKeySet.iterator();
		try{
			while (it.hasNext()) {
				String key = it.next();
				String value = catTable.get(key);
				int spaceBufferLength = Math.max(5, startParamIndex - key.length());
				// if nothing was set, do not add it to outfile
				if (value.equals("")) {
					continue;
				}
				b.write(key + spaceBuffer(spaceBufferLength) + value + "\n");
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
	private static void createOutputFile(String fileName, Hashtable<String,String> table, String fileType){
		try{
			File outFile = new File(fileName);
			if (!outFile.exists()) {
				outFile.createNewFile();
			}
			
			BufferedWriter b1 = new BufferedWriter(new FileWriter(outFile));	
			//Formatting Constants
			int settingsStartingIndex = 6; // spark.[starting index]*
			int recommendationsStartingIndex = 0; //Recommendation tag starts at index 0
			Hashtable <String, String> environmentKeySet = extractKeyCategory("environment", table, recommendationsStartingIndex);
			Hashtable <String, String> yarnKeySet = extractKeyCategory("yarn",table, settingsStartingIndex);
			Hashtable <String, String> streamingKeySet = extractKeyCategory("streaming",table, settingsStartingIndex);
			Hashtable <String, String> dynamicAllocationKeySet = extractKeyCategory("dynamicAllocation",table, settingsStartingIndex);

			if (fileType.equals("recommendations")){
				b1.write("#####################################################################################################################################################################"
						+ "\n#Cloudera Manager/Environment Settings\n"
						+ "#####################################################################################################################################################################"
						+ "\n\n");
				writeCategory(environmentKeySet, b1);
				b1.write("\n");
			}
			
			b1.write("#####################################################################################################################################################################"
					+ "\n#Standalone Mode Default Settings\n"
					+ "#####################################################################################################################################################################"
					+ "\n\n");
			writeCategory(table, b1);
			b1.write("\n"
					+ "#####################################################################################################################################################################"
					+ "\n#YARN Mode Default Settings (ignore if not using YARN)\n"
					+ "#####################################################################################################################################################################"
					+ "\n\n");
			writeCategory(yarnKeySet, b1);
			b1.write("\n"
					+ "#####################################################################################################################################################################"
					+ "\n#Streaming Default Settings (ignore if not using Spark Streaming)\n"
					+ "#####################################################################################################################################################################"
					+ "\n\n");
			writeCategory(streamingKeySet, b1);
			b1.write("\n"
					+ "#####################################################################################################################################################################"
					+ "\n#Dynamic Allocation Default Settings (ignore if not using Dynamic Allocation)\n"
					+ "#####################################################################################################################################################################"
					+ "\n\n");
			writeCategory(dynamicAllocationKeySet, b1);
			b1.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void printUsage() {
		System.out.println("Usage: \n"
				+ "If no arguments are put, follow prompts.. \n"
				+ "./run.sh \n"
				+ "<input data size in GB> \n"
				+ "<number of nodes in cluster including master> \n"
				+ "<number of cores per node> \n"
				+ "<memory per node in GB - for Standalone its the node, for yarn its the container size> \n"
				+ "<fraction of resources used 0-1.0> \n"
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
