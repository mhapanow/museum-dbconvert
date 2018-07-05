package com.ciessa.museum.dbconvert;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DBConvertService {

	private static final Logger log = Logger.getLogger(DBConvertService.class.getName());
	
	public static final Integer STEP_FD = 1;
	public static final Integer STEP_DDL = 2;
	public static final Integer STEP_COPY = 3;
	
	public void doConvert( Properties config, List<Integer> steps, List<String> forcedFiles ) throws Exception {
		
		Connection sqlConn = null;
		Connection iSeriesConn = null;
		
		try {
			sqlConn = getSQLConnection(config);
			iSeriesConn = getISeriesConnection(config);

			if(steps.isEmpty() || steps.contains(STEP_FD)) createWorkingEnvironment(config, iSeriesConn);
			if(steps.isEmpty() || steps.contains(STEP_DDL)) createSQLDDL(config, forcedFiles, iSeriesConn, sqlConn);
			if(steps.isEmpty() || steps.contains(STEP_COPY)) copyData(config, forcedFiles, iSeriesConn, sqlConn);

		} catch( Exception e ) {
			log.log(Level.SEVERE, e.getMessage(), e);
			throw new Exception(e);
		} finally {
			if (sqlConn != null) try { sqlConn.close(); } catch(Exception e) {}  
			if (iSeriesConn != null) try { iSeriesConn.close(); } catch(Exception e) {}  
		}
		
	}

	public void copyData( Properties config, List<String> forcedFiles, Connection iSeriesConn, Connection sqlConn ) throws SQLException {

		String workLib = config.getProperty("iseries.workLib", Defaults.ISERIES_WORKLIB);
		List<String> ommitFiles = Arrays.asList(config.getProperty("iseries.ommitFiles", Defaults.ISERIES_OMMITFILES).split(","));
		Statement stmt = null;
		ResultSet rs = null;

		SimpleDateFormat sdfd = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdft = new SimpleDateFormat("HH:mm:ss.000");
		SimpleDateFormat sdfz = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.000");
		
		List<FieldDescription> fieldList = new ArrayList<>();
		log.log(Level.INFO, "copying data files...");
		
		try {
			String SQL = "SELECT DISTINCT MLNRCD, MLFILE, MLLIB, MLNAME FROM " + workLib + ".DSPFD WHERE MLNRCD > 0 ORDER BY MLLIB, MLFILE, MLNAME";  
			stmt = iSeriesConn.createStatement();  
			rs = stmt.executeQuery(SQL);
			while( rs.next()) {
				String file = rs.getString("MLFILE").trim();
				String library = rs.getString("MLLIB").trim();
				String member = rs.getString("MLNAME").trim();
				if(!ommitFiles.contains(file)) {

					if(forcedFiles.isEmpty() || forcedFiles.contains(file)) {

						log.log(Level.INFO, "Processing file " + library + "/" + file + " and member " + member + "...");

						call(config, iSeriesConn, "OVRDBF FILE(" + file + ") TOFILE(" + library + "/" + file +") MBR(" + member + ") OVRSCOPE(*JOB)");
						SQL = "SELECT DISTINCT WHFILE, WHLIB, WHFTYP, WHFLDE, WHFLDB, WHFLDD, WHFLDP, WHFTXT, WHFLDT FROM "
								+ workLib + ".DSPFFD WHERE WHFILE = '" + file + "' AND WHLIB = '" + library + "'";
						Statement stmt2 = iSeriesConn.createStatement();
						ResultSet rs2 = stmt2.executeQuery(SQL);
						fieldList.clear();

						// Iterate through the data in the result set and display it.  
						while (rs2.next()) {
							String fileType = rs2.getString("WHFTYP").trim();
							String field = rs2.getString("WHFLDE").trim();
							int lenght = rs2.getInt("WHFLDB");
							int digits = rs2.getInt("WHFLDD");
							int decimals = rs2.getInt("WHFLDP");
							String text = rs2.getString("WHFTXT").trim();
							String fieldType = rs2.getString("WHFLDT").trim();

							FieldDescription fd = new FieldDescription(file, library, fileType, field, lenght, digits, decimals, text, fieldType);
							fieldList.add(fd);
						}

						rs2.close();
						stmt2.close();

						// Deletes the previous records
						Statement del = sqlConn.createStatement();
						del.execute("DELETE FROM " + file + " WHERE MEMBER='" + member + "'");
						del.close();

						// Begins batch execution
						sqlConn.setAutoCommit(false);
						Statement w = sqlConn.createStatement();  
						long recCount = 0;
						
						// Selects the records and inserts them in SQL Server
						SQL = "SELECT * FROM " + file;
						stmt2 = iSeriesConn.createStatement();
						rs2 = stmt2.executeQuery(SQL);

						// Iterate through the records to insert  
						while (rs2.next()) {
							StringBuffer sb = new StringBuffer();
							sb.append("INSERT INTO " + file + "( PKID, MEMBER");
							for(FieldDescription fd : fieldList)
								sb.append(",\"").append(fd.getField()).append("\"");
							sb.append(") values ('" + UUID.randomUUID().toString() + "'");
							sb.append(",'" + member + "'");
							for(FieldDescription fd : fieldList) {
								sb.append(",");
								switch (fd.getFieldType()) { 
								case "A":
									String v = rs2.getString(fd.getOriginalField());
									v = v.replaceAll("'", "''");
//									sb.append("'").append(v.trim()).append("'");
									sb.append("'").append(v.replaceAll("\\s+$","")).append("'");
									break;
								case "S":
								case "P":
								case "B":
									if(fd.getDecimals() == 0 ) {
										long v1 = rs2.getLong(fd.getOriginalField());
										sb.append(v1);
									} else {
										double v1 = rs2.getDouble(fd.getOriginalField());
										sb.append(v1);
									}
									break;
								case "L":
									Date d = rs2.getDate(fd.getOriginalField());
									sb.append("'").append(sdfd.format(d)).append("'");
									break;
								case "T":
									Time t = rs2.getTime(fd.getOriginalField());
									sb.append("'").append(sdft.format(t)).append("'");
									break;
								case "Z":
									Date z = rs2.getTimestamp(fd.getOriginalField());
									sb.append("'").append(sdfz.format(z)).append("'");
									break;
								default:
								}
							}
							sb.append(")");

							// Executes the insert
							recCount++;
							w.addBatch(sb.toString());
							if( recCount % 1000 == 0 ) {
								System.out.println("inserting " + recCount + " records...");
								recCount = 0;
								w.executeBatch();
								sqlConn.commit();
								w.close();
								w = sqlConn.createStatement();  
							}
							sqlConn.setAutoCommit(true);
							
						}

						if( recCount > 0 ) {
							System.out.println("inserting " + recCount + " records...");
							w.executeBatch();
							sqlConn.commit();
							w.close();
						}
						
						call(config, iSeriesConn, "DLTOVR FILE(" + file + ") LVL(*JOB)");

					}
				}
			}

		} finally {
			if (rs != null ) try {rs.close();} catch(Exception e) {}
			if (stmt != null ) try {stmt.close();} catch(Exception e) {}
		}

	}
	
	public void createSQLDDL( Properties config, List<String> forcedFiles, Connection iSeriesConn, Connection sqlConn ) throws SQLException {

		String workLib = config.getProperty("iseries.workLib", Defaults.ISERIES_WORKLIB);
		String owner = config.getProperty("mssql.owner", Defaults.MSSQL_OWNER);
		Boolean createOnlyFilesWithData = Boolean.parseBoolean(config.getProperty("iseries.createOnlyFilesWithData", Defaults.ISERIES_CREATEONLYFILESWITHDATA));
		List<String> ommitFiles = Arrays.asList(config.getProperty("iseries.ommitFiles", Defaults.ISERIES_OMMITFILES).split(","));
		List<String> configForcedFiles = Arrays.asList(config.getProperty("iseries.forcedFiles", Defaults.ISERIES_FORCEDFILES).split(","));
		Statement stmt = null;
		ResultSet rs = null;
		Set<String> validFiles = new HashSet<String>();
		Set<String> alreadyOmmited = new HashSet<String>();

		log.log(Level.INFO, "Creating DDLs...");

		try {
			String SQL = "SELECT * FROM " + workLib + ".DSPFD WHERE MLFTYP = 'P'";  
			stmt = iSeriesConn.createStatement();  
			rs = stmt.executeQuery(SQL);
			while( rs.next()) {
				int records = rs.getInt("MLNRCD");
				String file = rs.getString("MLFILE").trim();
				if( forcedFiles.isEmpty() || forcedFiles.contains(file)) {
					if (records > 0 || !createOnlyFilesWithData || configForcedFiles.contains(file))
						validFiles.add(file);
				}
			}
			rs.close();
			stmt.close();
			
			for( String ommitFile : ommitFiles ) {
				validFiles.remove(ommitFile);
			}
			
			SQL = "SELECT DISTINCT WHFILE, WHLIB, WHFTYP, WHFLDE, WHFLDB, WHFLDD, WHFLDP, WHFTXT, WHFLDT FROM "
					+ workLib + ".DSPFFD WHERE WHFTYP = 'P' AND WHFILE IN (" + toSQLList(validFiles) + ")";
			stmt = iSeriesConn.createStatement();
			rs = stmt.executeQuery(SQL);
			String lastFile = null;
			
			StringBuffer command = null;
			int columnCount = 0;
			// Iterate through the data in the result set and display it.  
			while (rs.next()) {
				String file = rs.getString("WHFILE").trim();
				String library = rs.getString("WHLIB").trim();
				String fileType = rs.getString("WHFTYP").trim();
				String field = rs.getString("WHFLDE").trim();
				int lenght = rs.getInt("WHFLDB");
				int digits = rs.getInt("WHFLDD");
				int decimals = rs.getInt("WHFLDP");
				String text = rs.getString("WHFTXT").trim();
				String fieldType = rs.getString("WHFLDT").trim();

				if( validFiles.contains(file) ) {
					if( fileType.equals("P")) {
						FieldDescription fd = new FieldDescription(file, library, fileType, field, lenght, digits, decimals, text, fieldType);
						if( !fd.getFile().equals(lastFile)) {
							if(lastFile != null ) {
								if( command != null) {
									command.append(", CONSTRAINT [PK_" + lastFile + "_1] PRIMARY KEY CLUSTERED \r\n" + 
											"(\r\n" + 
											"	[PKID] ASC\r\n" + 
											")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]\r\n" + 
											") ON [PRIMARY]");
									if( columnCount < 1024 ) {
										executeSQLCreateTable(config, sqlConn, lastFile, command.toString());
									}
								}
							}
							command = new StringBuffer();
							command.append("CREATE TABLE ").append(owner).append(".").append("\"" + fd.getFile() + "\"").append("( ");
							command.append("PKID VARCHAR(36), ");
							command.append("MEMBER VARCHAR(10)");

							lastFile = fd.getFile();
							columnCount = 0;
						}

						columnCount++;
						switch (fd.getFieldType()) { 
						case "A":
							if( fd.getLenght() <= 5000 ) {
								command.append(", ").append("\"" + fd.getField() + "\"").append(" VARCHAR(").append(fd.getLenght()).append(")");
							} else {
								command.append(", ").append("\"" + fd.getField() + "\"").append(" TEXT");
							}

							break;
						case "S":
						case "P":
						case "B":
							command.append(", ").append("\"" + fd.getField() + "\"");
							if(fd.getDecimals() == 0 ) {
								if( fd.getDigits() <= 9 ) {
									command.append(" INT");
								} else {
									command.append(" BIGINT");
								}
							} else {
								command.append(" DECIMAL(").append(fd.getDigits()).append(",").append(fd.getDecimals()).append(")");
							}
							break;
						case "L":
							command.append(", ").append("\"" + fd.getField() + "\"").append(" DATE");
							break;
						case "T":
							command.append(", ").append("\"" + fd.getField() + "\"").append(" TIME");
							break;
						case "Z":
							command.append(", ").append("\"" + fd.getField() + "\"").append(" DATETIME");
							break;
						default:
						}
					}
				}  else {
					if(!alreadyOmmited.contains(file)) {
						log.log(Level.INFO, "Ommiting file " + file + "...");
						alreadyOmmited.add(file);
					}
				}
			}
			
			if( command != null) {
				command.append(", CONSTRAINT [PK_" + lastFile + "_1] PRIMARY KEY CLUSTERED \r\n" + 
						"(\r\n" + 
						"	[PKID] ASC\r\n" + 
						")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]\r\n" + 
						") ON [PRIMARY]");
				if( columnCount < 1024 ) {
					executeSQLCreateTable(config, sqlConn, lastFile, command.toString());
				}
			}

		} finally {
			if (rs != null ) try {rs.close();} catch(Exception e) {}
			if (stmt != null ) try {stmt.close();} catch(Exception e) {}
		}

	}
	
	public String toSQLList(Set<String> input) {

		StringBuffer sb = new StringBuffer();
		Iterator<String> i = input.iterator();
		boolean isFirst = true;
		while(i.hasNext()) {
			String obj = i.next();
			if(!isFirst) sb.append(",");
			isFirst = false;
			sb.append("'").append(obj).append("'");
		}

		return sb.toString();

	}
	
	public void executeSQLCreateTable( Properties config, Connection sqlConnection, String tableName, String ddl) throws SQLException {
		Statement stmt = null;
		
		if (Boolean.parseBoolean(config.getProperty("mssql.dropTables", Defaults.MSSQL_DROPTABLES))) {
			StringBuffer sb = new StringBuffer();
			sb.append("if exists (select * from sysobjects where name='").append(tableName).append("' and xtype='U') drop table ").append("\"" + tableName + "\"");
			stmt = sqlConnection.createStatement();  
			stmt.execute(sb.toString());
			stmt.close();
		}
		
		log.log(Level.INFO, "Creating table " + tableName + "...");
		
		StringBuffer sb = new StringBuffer();
		sb.append("if not exists (select * from sysobjects where name='").append(tableName).append("' and xtype='U') ");
		sb.append(ddl);
		stmt = sqlConnection.createStatement();  
		stmt.execute(sb.toString());
		stmt.close();

	}
	
	public void createWorkingEnvironment( Properties config, Connection iSeriesConn ) throws SQLException {

		String workLib = config.getProperty("iseries.workLib", Defaults.ISERIES_WORKLIB);
		List<String> dataLibs = Arrays.asList(config.getProperty("iseries.dataLibs", Defaults.ISERIES_DATALIBS).split(","));
		List<String> transformedFiles = Arrays.asList(config.getProperty("iseries.transformedFiles", Defaults.ISERIES_TRANSFORMEDFILES).split(","));
		
		// Create working Library
		try {
			log.log(Level.INFO, "Checking for Work Library " + workLib + "...");
			call(config, iSeriesConn, "CHKOBJ OBJ(" + workLib + ") OBJTYPE(*LIB)");
		} catch( SQLException e ) {
			log.log(Level.INFO, "Creating Work Library " + workLib + "...");
			call(config, iSeriesConn, "CRTLIB LIB(" + workLib + ") TYPE(*TEST) TEXT('Working Library for Museum')");
		}

		// Runs the Files Transformations
		try {
			call(config, iSeriesConn, "ADDLIBLE LIB(" + workLib + ")");
		} catch( SQLException e ) {}
		for( String dataLib : dataLibs ) {
			try {
				call(config, iSeriesConn, "ADDLIBLE LIB(" + dataLib + ")");
			} catch( SQLException e ) {}
		}
		call(config, iSeriesConn, "CALL PGM(" + workLib + "/TRANSFC)");
		
		// Builds the File Description Object
		try {
			call(config, iSeriesConn, "CHKOBJ OBJ(" + workLib + "/DSPFD) OBJTYPE(*FILE)");
			call(config, iSeriesConn, "CLRPFM FILE(" + workLib + "/DSPFD)");
		} catch( SQLException e ) {}
		for( String dataLib : dataLibs ) {
			log.log(Level.INFO, "Creating File Description File for Library " + dataLib + "...");
			call(config, iSeriesConn, "DSPFD FILE(" + dataLib + "/*ALL) TYPE(*MBRLIST) OUTPUT(*OUTFILE) OUTFILE(" + workLib + "/DSPFD) OUTMBR(*FIRST *ADD)");
		}
		for( String transformedFile : transformedFiles ) {
			log.log(Level.INFO, "Creating File Description File for File " + transformedFile + "...");
			call(config, iSeriesConn, "DSPFD FILE(" + workLib + "/" + transformedFile + ") TYPE(*MBRLIST) OUTPUT(*OUTFILE) OUTFILE(" + workLib + "/DSPFD) OUTMBR(*FIRST *ADD)");
		}

		// Builds the Fields for File Description Object
		try {
			call(config, iSeriesConn, "CHKOBJ OBJ(" + workLib + "/DSPFFD) OBJTYPE(*FILE)");
			call(config, iSeriesConn, "CLRPFM FILE(" + workLib + "/DSPFFD)");
		} catch( SQLException e ) {}
		for( String dataLib : dataLibs ) {
			log.log(Level.INFO, "Creating Fields Description File for Library " + dataLib + "...");
			call(config, iSeriesConn, "DSPFFD FILE(" + dataLib + "/*ALL) OUTPUT(*OUTFILE) OUTFILE(" + workLib + "/DSPFFD) OUTMBR(*FIRST *ADD)");
		}
		for( String transformedFile : transformedFiles ) {
			log.log(Level.INFO, "Creating File Description File for File " + transformedFile + "...");
			call(config, iSeriesConn, "DSPFFD FILE(" + workLib + "/" + transformedFile + ") OUTPUT(*OUTFILE) OUTFILE(" + workLib + "/DSPFFD) OUTMBR(*FIRST *ADD)");
		}

	}

	public void call( Properties config, Connection conn, String source ) throws SQLException {

		CallableStatement cs = null;

		try {
			String command = buildCommand(source);
			cs = conn.prepareCall(command);
			cs.execute();
		} finally {
			if( cs != null ) try {cs.close();} catch (Exception e) {}
		}
		
	}
	
	public String buildCommand(String source) {

		int len = source.length();
		String command = escape(source);
		return "{CALL QCMDEXC ('" + command + "', " + len + ")}";
		
	}
	
	public String escape(String source) {

		String dst = new String(source);
		dst = dst.replaceAll("'", "''");
		return dst;

	}
	
	public Connection getISeriesConnection( Properties config ) throws ClassNotFoundException, SQLException {

		// Connects to the SQL Server instance
		// Create a variable for the connection string.
		String connectionUrl = "jdbc:as400://" 
				+ config.getProperty("iseries.server", Defaults.ISERIES_SERVER) + ";"
				+ "user=" + config.getProperty("iseries.user", Defaults.ISERIES_USER) + ";"
				+ "password=" + config.getProperty("iseries.password", Defaults.ISERIES_PASSWORD);

		
		// Establish the connection.  
		Class.forName("com.ibm.as400.access.AS400JDBCDriver");
		Connection con = DriverManager.getConnection(connectionUrl);
		con.setAutoCommit(true);
		
		return con;

	}
	
	public Connection getSQLConnection( Properties config ) throws ClassNotFoundException, SQLException {

		// Connects to the SQL Server instance
		// Create a variable for the connection string.
		String connectionUrl = "jdbc:sqlserver://" 
				+ config.getProperty("mssql.server", Defaults.MSSQL_SERVER) + ":"
				+ config.getProperty("mssql.port", Defaults.MSSQL_PORT) + ";"
				+ "databaseName=" + config.getProperty("mssql.database", Defaults.MSSQL_DATABASE) + ";"
				+ "user=" + config.getProperty("mssql.user", Defaults.MSSQL_USER) + ";"
				+ "password=" + config.getProperty("mssql.password", Defaults.MSSQL_PASSWORD);

		// Establish the connection.  
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");  
		Connection con = DriverManager.getConnection(connectionUrl);
		con.setAutoCommit(true);
		
		return con;

	}
}
