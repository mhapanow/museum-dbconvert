package com.ciessa.museum.dbconvert;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DBConvertService {

	private static final Logger log = Logger.getLogger(DBConvertService.class.getName());
	
	public void doConvert( Properties config ) throws Exception {
		
		Connection sqlConn = null;
		Connection iSeriesConn = null;
		
		try {
			sqlConn = getSQLConnection(config);
			iSeriesConn = getISeriesConnection(config);

//			createWorkingEnvironment(config, iSeriesConn);
//			createSQLDDL(config, iSeriesConn, sqlConn);
			copyData(config, iSeriesConn, sqlConn);
			

		} catch( Exception e ) {
			log.log(Level.SEVERE, e.getMessage(), e);
			throw new Exception(e);
		} finally {
			if (sqlConn != null) try { sqlConn.close(); } catch(Exception e) {}  
			if (iSeriesConn != null) try { iSeriesConn.close(); } catch(Exception e) {}  
		}
		
	}

	public void copyData( Properties config, Connection iSeriesConn, Connection sqlConn ) throws SQLException {
	}
	
	public void createSQLDDL( Properties config, Connection iSeriesConn, Connection sqlConn ) throws SQLException {

		String workLib = config.getProperty("iseries.workLib", Defaults.ISERIES_WORKLIB);
		String owner = config.getProperty("mssql.owner", Defaults.MSSQL_OWNER);
		Statement stmt = null;
		ResultSet rs = null;

		try {
			String SQL = "SELECT * FROM " + workLib + ".DSPFFD";  
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

				if( fileType.equals("P")) {
					FieldDescription fd = new FieldDescription(file, library, fileType, field, lenght, digits, decimals, text, fieldType);
					if( !fd.getFile().equals(lastFile)) {
						if(lastFile != null ) {
							if( command != null) {
								command.append(")");
								if( columnCount < 1024 ) {
									executeSQLCreateTable(config, sqlConn, lastFile, command.toString());
								}
							}
						}
						command = new StringBuffer();
						command.append("CREATE TABLE ").append(owner).append(".").append("\"" + fd.getFile() + "\"").append("( ");
						command.append("member VARCHAR(10)");
						
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
			}  
		} finally {
			if (rs != null ) try {rs.close();} catch(Exception e) {}
			if (stmt != null ) try {stmt.close();} catch(Exception e) {}
		}

	}
	
	public void executeSQLCreateTable( Properties config, Connection sqlConnection, String tableName, String ddl) throws SQLException {
		Statement stmt = null;
		
		if (Boolean.parseBoolean(config.getProperty("mssql.dropTables", Defaults.MSSQL_DROPTABLES))) {
			StringBuffer sb = new StringBuffer();
			sb.append("if exists (select * from sysobjects where name='").append(tableName).append("' and xtype='U') drop table ").append("\"" + tableName + "\"");
			System.out.println(sb.toString());
			stmt = sqlConnection.createStatement();  
			stmt.execute(sb.toString());
			stmt.close();
		}
		
		StringBuffer sb = new StringBuffer();
		sb.append("if not exists (select * from sysobjects where name='").append(tableName).append("' and xtype='U') ");
		sb.append(ddl);
		System.out.println(sb.toString());
		stmt = sqlConnection.createStatement();  
		stmt.execute(sb.toString());
		stmt.close();

	}
	
	public void createWorkingEnvironment( Properties config, Connection iSeriesConn ) throws SQLException {

		String workLib = config.getProperty("iseries.workLib", Defaults.ISERIES_WORKLIB);
		List<String> dataLibs = Arrays.asList(config.getProperty("iseries.dataLibs", Defaults.ISERIES_DATALIBS).split(","));
		
		// Create working Library
		try {
			log.log(Level.INFO, "Checking for Work Library " + workLib + "...");
			call(config, iSeriesConn, "CHKOBJ OBJ(" + workLib + ") OBJTYPE(*LIB)");
		} catch( SQLException e ) {
			log.log(Level.INFO, "Creating Work Library " + workLib + "...");
			call(config, iSeriesConn, "CRTLIB LIB(" + workLib + ") TYPE(*TEST) TEXT('Working Library for Museum')");
		}

		// Builds the File Description Object
		try {
			call(config, iSeriesConn, "CHKOBJ OBJ(" + workLib + "/DSPFD) OBJTYPE(*FILE)");
			call(config, iSeriesConn, "DLTF FILE(" + workLib + "CZAWKUY/DSPFD)");
		} catch( SQLException e ) {}
		for( String dataLib : dataLibs ) {
			log.log(Level.INFO, "Creating File Description File for Library " + dataLib + "...");
			call(config, iSeriesConn, "DSPFD FILE(" + dataLib + "/*ALL) TYPE(*MBRLIST) OUTPUT(*OUTFILE) OUTFILE(" + workLib + "/DSPFD) OUTMBR(*FIRST *ADD)");
		}

		// Builds the Fields for File Description Object
		try {
			call(config, iSeriesConn, "CHKOBJ OBJ(" + workLib + "/DSPFFD) OBJTYPE(*FILE)");
			call(config, iSeriesConn, "DLTF FILE(" + workLib + "CZAWKUY/DSPFFD)");
		} catch( SQLException e ) {}
		for( String dataLib : dataLibs ) {
			log.log(Level.INFO, "Creating Fields Description File for Library " + dataLib + "...");
			call(config, iSeriesConn, "DSPFFD FILE(" + dataLib + "/*ALL) OUTPUT(*OUTFILE) OUTFILE(" + workLib + "/DSPFFD) OUTMBR(*FIRST *ADD)");
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
