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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;
import streaming.src.main.Streaming;
import core.src.main.common.Common;
import core.src.main.yarn.Yarn;
import dynamicallocation.src.main.DynamicAllocation;

public class ConfigurationConsole {
	
	public static void main(String[] args) {
		
		/** 
		 * Simple tool to initialize and configure Spark config params, generate the command line and advice
		 */
		
		// input config parameters
		String instanceType = ""; // c3.4xlarge, c4.4xlarge, r3.2xlarge, m4.4xlarge
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
		String codePath = "";
		String appJar = ""; // app Jar URL
		String appArgs = ""; // app args as a single string
	
		//instanceType maps
		HashMap<String, String> instanceTypeMap = new HashMap<String, String>();
		instanceTypeMap.put("c3.4xlarge", "16:30");
		instanceTypeMap.put("c4.4xlarge", "16:30");
		instanceTypeMap.put("r3.2xlarge", "8:61");
		instanceTypeMap.put("m4.4xlarge", "16:64");

		//input table
		Hashtable<String, String> inputsTable = new Hashtable<String, String>();
		
		//output tables
		Hashtable<String, String> optionsTable = new Hashtable<String, String>();
		Hashtable<String, String> defaultOptionsTable = new Hashtable<String, String>();
		Hashtable<String, String> recommendationsTable = new Hashtable<String, String>();
		Hashtable<String, String> commandLineParamsTable = new Hashtable<String, String>();
		
		//file names
		String sparkDefaultConf = "spark-default.conf";
		String sparkFinalConf = "output/spark-final.conf";
		String sparkConfAdvice = "output/spark-conf.advice";
		String codeFilePath = "tmp-code-file-path.txt";
	
		//legal input arguments
		String [] legalInstanceTypes = {"c3.4xlarge", "c4.4xlarge", "r3.2xlarge", "m4.4xlarge"};
		String [] legalFileSystemInput = {"ext3","ext4","xfs"};
		String [] legalDeployModeInput = {"client","cluster"};
		String [] legalClusterManagerInput = {"standalone","yarn"};
		String [] legalYesNoInput = {"y","n"};

		inputDataSize = "1000";
		numNodes = "7";
		resourceFraction = "1.0";
		fileSystem = "ext4";
		master = "dummy";
		deployMode = "client";
		clusterManager = "yarn";
		dynamicAllocationFlag = "y";
		className = "dummy";
		codePath = "dummy";
		appJar = "dummy";
		appArgs = "dummy";

		//get input parameters
		if (args.length == 0){
			printUsage();
			Scanner scanner = new Scanner(System.in);
			instanceType = checkValidHelper("Enter instance type (c3.4xlarge, c4.4xlarge, r3.2xlarge or m4.4xlarge): ",
					legalInstanceTypes, "Invalid input. Enter c3.4xlarge/c4.4xlarge/r3.2xlarge/m4.4xlarge", scanner);
			String[] mapInfo = instanceTypeMap.get(instanceType).split(":");
			numCoresPerNode = mapInfo[0];
			memoryPerNode = mapInfo[1];
		} else if(args.length != 1) {
			System.out.println("Invalid input, please enter instance type. \n");
			System.out.println("Your input: " + Arrays.toString(args) + "\n");	
			printUsage();
			System.exit(0);
		} else {
			boolean invalidFlag = false;
			instanceType = args[0];
			if (checkLegalInputs(instanceType, legalInstanceTypes, "Invalid instance type:")) {invalidFlag = true;}
			if (invalidFlag){
				System.out.println("Please re-enter arguments properly. \n");
				printUsage();
				System.exit(0);
			}
			String[] mapInfo = instanceTypeMap.get(instanceType).split(":");
			numCoresPerNode = mapInfo[0];
			memoryPerNode = mapInfo[1];
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
                defaultOptionsTable.put(optionKey, optionValue);
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
		
		createOutputFile(sparkFinalConf, optionsTable, defaultOptionsTable, "options");
		createOutputFile(sparkConfAdvice, recommendationsTable, defaultOptionsTable, "recommendations");
		
		System.out.println("Auto-generated files in output folder: spark-final.conf, spark.conf.advice \n");
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
	
	private static void writeCategory(Hashtable <String, String> catTable , Hashtable<String,String> defaultTable, BufferedWriter b, String fileType){
		int startParamIndex = 71;
		ArrayList <String> tableKeySet = new ArrayList<String>(catTable.keySet());
		Collections.sort(tableKeySet);
		Iterator <String> it = tableKeySet.iterator();
		try{
			while (it.hasNext()) {
				String key = it.next();
				String value = catTable.get(key);
				int spaceBufferLength = Math.max(5, startParamIndex - key.length());
				if (value.equals("")) { // if nothing was set, do not add it to outfile
					continue;
				}
				if (fileType.equals("options")){
					String defaultValue = defaultTable.get(key);
					if (defaultValue == null || !defaultValue.equals(value)){
						b.write("###################################################################### modified:\n");
					}
				}

				b.write(key + spaceBuffer(spaceBufferLength) + value + "\n");
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private static void createCodePathFile(String fileName, String filePath){
		try{
			File outFile = new File(fileName);
			if (!outFile.exists()) {
				outFile.createNewFile();
			}
			BufferedWriter b1 = new BufferedWriter(new FileWriter(outFile));	
			filePath = filePath.trim();
			b1.write(filePath);
			b1.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private static void createOutputFile(String fileName, Hashtable<String,String> table, Hashtable<String,String> defaultTable, String fileType){
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
				writeCategory(environmentKeySet, defaultTable, b1, fileType);
				b1.write("\n"
						+ "#################################################################################################################"
						+ "\n#Cloudera Manager/Environment Settings\n"
						+ "#################################################################################################################"
						+ "\n\n");
				writeCategory(environmentCMKeySet, defaultTable, b1, fileType);
				b1.write("\n");
			}
			
			b1.write("#################################################################################################################"
					+ "\n#Common Settings\n"
					+ "#################################################################################################################"
					+ "\n\n");
			writeCategory(table, defaultTable, b1, fileType);
			b1.write("\n"
					+ "#################################################################################################################"
					+ "\n#YARN Settings (ignore if not using YARN)\n"
					+ "#################################################################################################################"
					+ "\n\n");
			writeCategory(yarnKeySet, defaultTable, b1, fileType);
			b1.write("\n"
					+ "#################################################################################################################"
					+ "\n#Streaming Settings (ignore if not using Spark Streaming)\n"
					+ "#################################################################################################################"
					+ "\n\n");
			writeCategory(streamingKeySet, defaultTable, b1, fileType);
			b1.write("\n"
					+ "#################################################################################################################"
					+ "\n#Dynamic Allocation Settings (ignore if not using Dynamic Allocation)\n"
					+ "#################################################################################################################"
					+ "\n\n");
			writeCategory(dynamicAllocationKeySet, defaultTable, b1, fileType);
			b1.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void printUsage() {
		System.out.println("USAGE: \n"
				+ "./run.sh \n"
				+ "<input instance type (c3.4xlarge/c4.4xlarge/r3.2xlarge/m4.4xlarge)> \n"
				+ "\n"
				+ "e.g ./run.sh m4.4xlarge\n"
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
		
		System.out.println("Auto-generated files in output folder: spark-final.conf, spark.conf.advice, optimization-report.txt, optimizedCode.scala, spark.code.advice \n");
		System.out.println("Invoke command line: " + cmdLine + "\n");
	}
	
	public static String errorIntCheck (String prompt, String errorMsg, Scanner scanner ){
		String input = "";
		while (true){
			System.out.print(prompt + "\n");
			input = scanner.nextLine();
			if (checkIllegalInt(input, errorMsg)){
				continue;
			}else{
				break;
			}
		}
		return input;
	}
	
	public static String errorResourceFractionCheck (String prompt, String errorMsg, Scanner scanner ){
		String input = "";
		while (true){
			System.out.print(prompt + "\n");
			input = scanner.nextLine();
			if (checkIllegalDouble(input, errorMsg)){
				continue;
			}else{
				break;
			}
		}
		return input;
	}
	
	public static String scanNextWithPrompt (String prompt, Scanner scanner){
		System.out.print(prompt + "\n");
		String input = scanner.nextLine();
		return input;
	}
	
	public static String checkValidHelper (String prompt, String[] options, String errorMsg, Scanner scanner){
		String input = "";
		while (true){
			System.out.print(prompt + "\n");
			input = scanner.nextLine();
			boolean validFlag = false;
			for (int i = 0; i < options.length; i++){
				if (input.equals(options[i])){
					validFlag = true;
				}
			}
			if (validFlag){
				break;
			}
			else{
				System.out.println(errorMsg+ "\n");
			}
		}
		return input;
	}
	
	public static boolean checkIllegalInt(String arg, String errorMsg){
		boolean flag = false;
		try {
			int inputInt = Integer.parseInt(arg);
			if (inputInt <= 0){
				System.out.println(errorMsg + " [" +arg + "]");
				flag = true;
			}
		} catch (Exception e){
			System.out.println(errorMsg + " [" +arg + "]");
			flag = true;
		}
		return flag;
	}
	
	public static boolean checkIllegalDouble(String arg, String errorMsg){
		boolean flag = false;
		try {
			double inputDouble = Double.parseDouble(arg);
			if (inputDouble <= 0 || inputDouble > 1){
				System.out.println(errorMsg + " [" +arg + "]");
				flag = true;
			}
		} catch (Exception e){
			System.out.println(errorMsg + " [" +arg + "]");
			flag = true;
		}
		return flag;
	}
	
	public static boolean checkLegalInputs(String arg, String[] legalOptions, String errorMsg){
		boolean flag = false;
		for (int i = 0; i < legalOptions.length; i++){
			if (arg.equals(legalOptions[i])){
				flag = true;
			}
		}
		if (!flag){
			System.out.println(errorMsg + " [" +arg + "]");
		}
		return !flag;
	}
}
