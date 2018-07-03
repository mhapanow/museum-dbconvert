package com.ciessa.museum.dbconvert;

public class Defaults {

	// SQL Server Defaults
	public static String MSSQL_SERVER = "localhost";
	public static String MSSQL_PORT = "1433";
	public static String MSSQL_DATABASE = "Museum";
	public static String MSSQL_OWNER = "dbo";
	public static String MSSQL_USER = "museum";
	public static String MSSQL_PASSWORD = "museum";
	public static String MSSQL_DROPTABLES = "true";
	
	// iSeries Defaults
	public static String ISERIES_SERVER = "localhost";
	public static String ISERIES_USER = "museum";
	public static String ISERIES_PASSWORD = "museum";
	public static String ISERIES_WORKLIB = "CZAWKUY";
	public static String ISERIES_DATALIBS = "CZAPDCITBK,CZAPDUTIL";
	public static String ISERIES_TRANSFORMEDFILES = "Cfp001220";
	public static String ISERIES_OMMITFILES = "CFP001";
	public static String ISERIES_CREATEONLYFILESWITHDATA = "false";

}
