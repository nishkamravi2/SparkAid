package console.src.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Scanner;
import streaming.src.main.Streaming;
import core.src.main.common.Common;
import core.src.main.yarn.Yarn;
import dynamicallocation.src.main.DynamicAllocation;

public class ConfigurationConsole {
	
	public static void main(String[] args) {
		
		/** 
		 * Simple tool to initialize and configure Spark config params, generate the command line and advise
		 */
		
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
		String dynamicAllocationFlag = ""; // y, n
		String className = ""; // name of app class
		String appJar = ""; // app Jar URL
		String appArgs = ""; // app args as a single string
		
		//input table
		Hashtable<String, String> inputsTable = new Hashtable<String, String>();
		
		//output tables
		Hashtable<String, String> optionsTable = new Hashtable<String, String>();
		Hashtable<String, String> recommendationsTable = new Hashtable<String, String>();
		Hashtable<String, String> commandLineParamsTable = new Hashtable<String, String>();
		
		//file names
		String sparkDefaultConf = "spark-default.conf";
		String sparkFinalConf = "../output/spark-final.conf";
		String sparkConfAdvise = "../output/spark.conf.advise";
	
		//get input parameters
		if (args.length == 0){
			printUsage();
			Scanner scanner = new Scanner(System.in);
			inputDataSize = errorIntCheck("Enter input data size in GB",  
					"Invalid input. Please re-enter valid integer.", scanner);
			numNodes = errorIntCheck("Enter number of nodes in cluster (including master):",  
					"Invalid input. Please re-enter valid integer.", scanner);
			numCoresPerNode = errorIntCheck("Enter number of cores per node: ", 
					"Invalid input. Please re-enter valid integer.", scanner);
			memoryPerNode = errorIntCheck("Enter memory per node in GB: ", 
					"Invalid input. Please re-enter valid integer.", scanner);
			resourceFraction = errorResourceFractionCheck("Enter fraction of cluster resources for this job (from 0.0 to 1.0): ", 
					"Invalid input. Please re-enter valid double from 0.0 to 1.0.", scanner);
			fileSystem = fileSystemCheck("Enter file system of input raw data: ext3/ext4/xfs", 
					"Invalid input. Enter ext3/ext4/xfs.", scanner);
			master = scanNextWithPrompt("Enter master URL:", scanner);
			deployMode = deployModeCheck("Enter deploy mode: cluster / client", 
					"Invalid input. Enter cluster / client.", scanner);
			clusterManager = clusterManagerCheck("Enter Cluster Manager: standalone / yarn", "Invalid input. Enter standalone / yarn.", scanner);
			dynamicAllocationFlag = yesNoCheck("Is this a Dynamic Allocation application? y/n", 
					"Invalid input. Enter y/n.", scanner);
			className = scanNextWithPrompt("Enter Class Name of application: ", scanner);
			appJar = scanNextWithPrompt("Enter file path of application JAR ", scanner);
			appArgs = scanNextWithPrompt("Enter Application Arguments if any", scanner);
		}
		else if(args.length != 13) {
			System.out.println("Invalid input, please enter 13 arguments. \n");
			System.out.println("Your input: " + Arrays.toString(args) + "\n");	
			printUsage();
			System.exit(0);
		} else {
			inputDataSize = args[0];
			numNodes = args[1];
			numCoresPerNode = args[2];
			memoryPerNode = args[3];
			resourceFraction = args[4];
			fileSystem = args[5];
			master = args[6];
			deployMode = args[7];
			clusterManager = args[8];
			dynamicAllocationFlag = args[9];
			className = args[10];
			appJar = args[11];
			appArgs = args[12];
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
		inputsTable.put("dynamicAllocationFlag", dynamicAllocationFlag);
		inputsTable.put("className", className);
		inputsTable.put("appJar", appJar);
		inputsTable.put("appArgs", appArgs);

        String line = null;

        try {
            FileReader fileReader = new FileReader(sparkDefaultConf);
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
            System.out.println("Unable to open file '" + sparkDefaultConf + "'");                
        }
        catch(IOException ex) {
            System.out.println("Error reading file '" + sparkDefaultConf + "'");                   
        }
        
		//first initialize standard/standalone parameters
		Common.configureStandardSettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		
		//if it is yarn, add in the Yarn settings
		if (clusterManager.equals("yarn")){ Yarn.configureYarnSettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);}
		
		//configure necessary Dynamic Allocation settings
		if (dynamicAllocationFlag.equals("y")){ 
			DynamicAllocation.configureDynamicAllocationSettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		}

		//configure necessary Streaming settings
		Streaming.configureStreamingSettings(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		
		createOutputFile(sparkFinalConf, optionsTable, "options");
		createOutputFile(sparkConfAdvise, recommendationsTable, "recommendations");
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
			if (value.equals("")) { // if nothing was set, do not add it to output
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
		int startParamIndex = 71;
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
			Hashtable <String, String> environmentCMKeySet = extractKeyCategory("CM-environment", table, recommendationsStartingIndex);
			Hashtable <String, String> yarnKeySet = extractKeyCategory("yarn",table, settingsStartingIndex);
			Hashtable <String, String> streamingKeySet = extractKeyCategory("streaming",table, settingsStartingIndex);
			Hashtable <String, String> dynamicAllocationKeySet = extractKeyCategory("dynamicAllocation",table, settingsStartingIndex);
			
			if (fileType.equals("recommendations")){
				b1.write("#################################################################################################################"
						+ "\n#Environment Variables (Set directly)\n"
						+ "#################################################################################################################"
						+ "\n\n");
				writeCategory(environmentKeySet, b1);
				b1.write("\n"
						+ "#################################################################################################################"
						+ "\n#Cloudera Manager/Environment Settings\n"
						+ "#################################################################################################################"
						+ "\n\n");
				writeCategory(environmentCMKeySet, b1);
				b1.write("\n");
			}
			
			b1.write("#################################################################################################################"
					+ "\n#Common Settings\n"
					+ "#################################################################################################################"
					+ "\n\n");
			writeCategory(table, b1);
			b1.write("\n"
					+ "#################################################################################################################"
					+ "\n#YARN Mode Default Settings (ignore if not using YARN)\n"
					+ "#################################################################################################################"
					+ "\n\n");
			writeCategory(yarnKeySet, b1);
			b1.write("\n"
					+ "#################################################################################################################"
					+ "\n#Streaming Default Settings (ignore if not using Spark Streaming)\n"
					+ "#################################################################################################################"
					+ "\n\n");
			writeCategory(streamingKeySet, b1);
			b1.write("\n"
					+ "#################################################################################################################"
					+ "\n#Dynamic Allocation Default Settings (ignore if not using Dynamic Allocation)\n"
					+ "#################################################################################################################"
					+ "\n\n");
			writeCategory(dynamicAllocationKeySet, b1);
			b1.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void printUsage() {
		System.out.println("USAGE: \n"
				+ "./run.sh \n"
				+ "<input data size in GB> \n"
				+ "<number of nodes in cluster including master> \n"
				+ "<number of cores per node> \n"
				+ "<memory per node in GB>\n"
				+ "<fraction of resources used 0-1.0> \n"
				+ "<filesystem type> \n"
				+ "<master: standalone URL/yarn> \n"
				+ "<deployMode: cluster/client> \n"
				+ "<clusterManger: standalone/yarn> \n"
				+ "<dynamicAllocation: y/n> \n"
				+ "<app className> \n"
				+ "<app JAR location> \n"
				+ "<app arguments as one string>\n"
				+ "\n"
				+ "e.g ./run.sh 40 15 16 64 1.0 ext3 spark://hostname.com:7077 client standalone y Pagerank /path/to/spark.jar \"\"\n"
				+ "\n"
				+ "YOU CAN ALSO FOLLOW PROMPTS\n");
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
				+ " --properties-file spark-final.conf " 
				+ cmdLineParams + " " + appJar + " " + appArgs;
		
		System.out.println("Auto-generated files in output folder: spark-final.conf, spark.conf.advise \n");
		System.out.println("Invoke command line: " + cmdLine + "\n");
	}
	
	public static String errorIntCheck (String prompt, String errorMsg, Scanner scanner ){
		String input = "";
		while (true){
			System.out.print(prompt + "\n");
			input = scanner.nextLine();
			try {
				int inputInt = Integer.parseInt(input);
				if (inputInt <= 0){
					System.out.println(errorMsg+ "\n");
					continue;
				}
			} catch (Exception e){
				System.out.println(errorMsg+ "\n");
				continue;
			}
			break;
		}
		return input;
	}
	
	public static String errorResourceFractionCheck (String prompt, String errorMsg, Scanner scanner ){
		String input = "";
		while (true){
			System.out.print(prompt + "\n");
			input = scanner.nextLine();
			try {
				double inputDouble = Double.parseDouble(input);
				if (inputDouble <= 0 || inputDouble > 1.0){
					System.out.println(errorMsg+ "\n");
					continue;
				}
			} catch (Exception e){
				System.out.println(errorMsg+ "\n");
				continue;
			}
			break;
		}
		return input;
	}
	
	public static String fileSystemCheck (String prompt, String errorMsg, Scanner scanner ){
		String input = "";
		while (true){
			System.out.print(prompt + "\n");
			input = scanner.nextLine();
			if (input.equals("ext3") || input.equals("ext4") ||input.equals("xfs") ){
				break;
			}
			else{
				System.out.println(errorMsg+ "\n");
			}
		}
		return input;
	}
	
	public static String deployModeCheck (String prompt, String errorMsg, Scanner scanner ){
		String input = "";
		while (true){
			System.out.print(prompt + "\n");
			input = scanner.nextLine();
			if (input.equals("client") || input.equals("cluster")){
				break;
			}
			else{
				System.out.println(errorMsg+ "\n");
			}
		}
		return input;
	}
	
	public static String clusterManagerCheck (String prompt, String errorMsg, Scanner scanner ){
		String input = "";
		while (true){
			System.out.print(prompt + "\n");
			input = scanner.nextLine();
			if (input.equals("standalone") || input.equals("yarn")){
				break;
			}
			else{
				System.out.println(errorMsg+ "\n");
			}
		}
		return input;
	}
	
	public static String yesNoCheck (String prompt, String errorMsg, Scanner scanner ){
		String input = "";
		while (true){
			System.out.print(prompt + "\n");
			input = scanner.nextLine();
			if (input.equals("y") || input.equals("n")){
				break;
			}
			else{
				System.out.println(errorMsg+ "\n");
			}
		}
		return input;
	}
	
	public static String scanNextWithPrompt (String prompt, Scanner scanner ){
		System.out.print(prompt + "\n");
		String input = scanner.nextLine();
		return input;
	}
	
}
