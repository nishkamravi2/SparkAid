package security;

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

	
	public static void configureSecuritySettings(String[] args,
			Hashtable<String, String> optionsTable,
			Hashtable<String, String> recommendationsTable,
			Hashtable<String, String> commandLineParamsTable) {
		
		setEncryption(args, optionsTable, recommendationsTable, commandLineParamsTable);
		setSecurity(args, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	public static void setEncryption(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		setSSLEnabled(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setSSLEnabledAlgorithms(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setSSLKeyPassword(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setSSLKeyStore(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setSSLKeyStorePassword(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setSSLProtocol(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setSSLTrustStore(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setSSLTrustStorePassword(args, optionsTable, recommendationsTable, commandLineParamsTable);
	}

	public static void setSecurity(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable) {
		setSparkAclsEnable(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setAdminAcls(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setAuthenticate(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setAuthenticateSecret(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setCoreConnectionAckWaitTimeout(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setCoreConnectionAuthWaitTimeout(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setModifyAcls(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setUiFilters(args, optionsTable, recommendationsTable, commandLineParamsTable);
	    setUiViewAcls(args, optionsTable, recommendationsTable, commandLineParamsTable);
	}
	
	//Security
	private static void setSparkAclsEnable(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.acls.enable", sparkAclsEnable);
		recommendationsTable.put("spark.acls.enable", "default recommendation");
	}

	private static void setAdminAcls(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.admin.acls", adminAcls);
		recommendationsTable.put("spark.admin.acls", "default recommendation");
	}

	private static void setAuthenticate(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.authenticate", authenticate);
		recommendationsTable.put("spark.authenticate", "default recommendation");
	}

	private static void setAuthenticateSecret(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.authenticate.secret", authenticateSecret);
		recommendationsTable.put("spark.authenticate.secret", "default recommendation");
	}

	private static void setCoreConnectionAckWaitTimeout(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.core.connection.ack.wait.timeout", coreConnectionAckWaitTimeout);
		recommendationsTable.put("spark.core.connection.ack.wait.timeout", "default recommendation");
	}

	private static void setCoreConnectionAuthWaitTimeout(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.core.connection.auth.wait.timeout", coreConnectionAuthWaitTimeout);
		recommendationsTable.put("spark.core.connection.auth.wait.timeout", "default recommendation");
	}

	private static void setModifyAcls(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.modify.acls", modifyAcls);
		recommendationsTable.put("spark.modify.acls", "default recommendation");
	}

	private static void setUiFilters(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ui.filters", uiFilters);
		recommendationsTable.put("spark.ui.filters", "default recommendation");
	}

	private static void setUiViewAcls(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ui.view.acls", uiViewAcls);
		recommendationsTable.put("spark.ui.view.acls", "default recommendation");
	}
	
	//Encryption
	private static void setSSLEnabled(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ssl.enabled", sslEnbaled);
		recommendationsTable.put("spark.ssl.enabled", "default recommendation");
	}

	private static void setSSLEnabledAlgorithms(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ssl.enabledAlgorithms", sslEnabledAlgorithms);
		recommendationsTable.put("spark.ssl.enabledAlgorithms", "default recommendation");
	}

	private static void setSSLKeyPassword(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ssl.keyPassword", sslKeyPassword);
		recommendationsTable.put("spark.ssl.keyPassword", "default recommendation");
	}

	private static void setSSLKeyStore(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ssl.keyStore", sslKeyStore);
		recommendationsTable.put("spark.ssl.keyStore", "default recommendation");
	}

	private static void setSSLKeyStorePassword(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ssl.keyStorePassword", sslKeyStorePassword);
		recommendationsTable.put("spark.ssl.keyStorePassword", "default recommendation");
	}

	private static void setSSLProtocol(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ssl.protocol", sslProtocol);
		recommendationsTable.put("spark.ssl.protocol", "default recommendation");
	}

	private static void setSSLTrustStore(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ssl.trustStore", sslTrustStore);
		recommendationsTable.put("spark.ssl.trustStore", "default recommendation");
	}

	private static void setSSLTrustStorePassword(String[] args, Hashtable<String, String> optionsTable, Hashtable<String, String> recommendationsTable, Hashtable<String, String> commandLineParamsTable){
		optionsTable.put("spark.ssl.trustStorePassword", sslTrustStorePassword);
		recommendationsTable.put("spark.ssl.trustStorePassword", "default recommendation");
	}

}
