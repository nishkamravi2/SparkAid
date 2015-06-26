package security.src.main;

import java.util.Hashtable;

public class Security {
	
	//Security
	static String sparkAclsEnable = ""; //false
	static String adminAcls = ""; //empty
	static String authenticate = ""; //false
	static String authenticateSecret = ""; //None
	static String coreConnectionAckWaitTimeout = ""; //60s
	static String coreConnectionAuthWaitTimeout = ""; //30s
	static String modifyAcls = ""; //empty
	static String uiFilters = ""; //None
	static String uiViewAcls = ""; //empty
	
	//Encryption
	static String sslEnbaled = ""; //false
	static String sslEnabledAlgorithms = ""; //empty
	static String sslKeyPassword = "";
	static String sslKeyStore = "";
	static String sslKeyStorePassword = "";
	static String sslProtocol = "";
	static String sslTrustStore = "";
	static String sslTrustStorePassword = "";

	
	public static void configureSecuritySettings(Hashtable<String, String> inputsTable,
			Hashtable<String, String> optionsTable,
			Hashtable<String, String> recommendationsTable,
			Hashtable<String, String> commandLineParamsTable) {
		
		setEncryption(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
		setSecurity(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	public static void setEncryption(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		setSSLEnabled(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setSSLEnabledAlgorithms(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setSSLKeyPassword(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setSSLKeyStore(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setSSLKeyStorePassword(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setSSLProtocol(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setSSLTrustStore(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setSSLTrustStorePassword(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}

	public static void setSecurity(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		setSparkAclsEnable(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setAdminAcls(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setAuthenticate(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setAuthenticateSecret(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setCoreConnectionAckWaitTimeout(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setCoreConnectionAuthWaitTimeout(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setModifyAcls(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setUiFilters(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	    setUiViewAcls(inputsTable, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	//Security
	private static void setSparkAclsEnable(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.acls.enable", sparkAclsEnable);
		recommendationsTable.put("spark.acls.enable", "");
	}

	private static void setAdminAcls(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.admin.acls", adminAcls);
		recommendationsTable.put("spark.admin.acls", "");
	}

	private static void setAuthenticate(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.authenticate", authenticate);
		recommendationsTable.put("spark.authenticate", "");
	}

	private static void setAuthenticateSecret(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.authenticate.secret", authenticateSecret);
		recommendationsTable.put("spark.authenticate.secret", "");
	}

	private static void setCoreConnectionAckWaitTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.core.connection.ack.wait.timeout", coreConnectionAckWaitTimeout);
		recommendationsTable.put("spark.core.connection.ack.wait.timeout", "");
	}

	private static void setCoreConnectionAuthWaitTimeout(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.core.connection.auth.wait.timeout", coreConnectionAuthWaitTimeout);
		recommendationsTable.put("spark.core.connection.auth.wait.timeout", "");
	}

	private static void setModifyAcls(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.modify.acls", modifyAcls);
		recommendationsTable.put("spark.modify.acls", "");
	}

	private static void setUiFilters(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ui.filters", uiFilters);
		recommendationsTable.put("spark.ui.filters", "");
	}

	private static void setUiViewAcls(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ui.view.acls", uiViewAcls);
		recommendationsTable.put("spark.ui.view.acls", "");
	}
	
	//Encryption
	private static void setSSLEnabled(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ssl.enabled", sslEnbaled);
		recommendationsTable.put("spark.ssl.enabled", "");
	}

	private static void setSSLEnabledAlgorithms(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ssl.enabledAlgorithms", sslEnabledAlgorithms);
		recommendationsTable.put("spark.ssl.enabledAlgorithms", "");
	}

	private static void setSSLKeyPassword(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ssl.keyPassword", sslKeyPassword);
		recommendationsTable.put("spark.ssl.keyPassword", "");
	}

	private static void setSSLKeyStore(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ssl.keyStore", sslKeyStore);
		recommendationsTable.put("spark.ssl.keyStore", "");
	}

	private static void setSSLKeyStorePassword(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ssl.keyStorePassword", sslKeyStorePassword);
		recommendationsTable.put("spark.ssl.keyStorePassword", "");
	}

	private static void setSSLProtocol(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ssl.protocol", sslProtocol);
		recommendationsTable.put("spark.ssl.protocol", "");
	}

	private static void setSSLTrustStore(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ssl.trustStore", sslTrustStore);
		recommendationsTable.put("spark.ssl.trustStore", "");
	}

	private static void setSSLTrustStorePassword(Hashtable<String, String> inputsTable, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ssl.trustStorePassword", sslTrustStorePassword);
		recommendationsTable.put("spark.ssl.trustStorePassword", "");
	}

}