package com.ciessa.museum.dbconvert.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.ciessa.museum.dbconvert.DBConvertService;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class DBConvert {

	private static OptionParser parser;

	public static OptionParser buildOptionParser(OptionParser base) {
		if (base == null)
			parser = new OptionParser();
		else
			parser = base;
		parser.accepts("config", "Configuration File").withRequiredArg().ofType(String.class);
		parser.accepts("force", "Comma separated list of forced files").withRequiredArg().ofType(String.class);
		parser.accepts("steps", "Comma separated list of steps to run: 1=Generate FD,2=Create SQL DDLs,3=Copy Data").withRequiredArg().ofType(String.class);
		return parser;
	}

	public static void main(String args[]) {

		// Option parser help is in http://pholser.github.io/jopt-simple/examples.html
		parser = buildOptionParser(parser);
		OptionSet options = parser.parse(args);

		// Parses configuration from config file
		String configFileName;
		if (options.has("config")) {
			configFileName = (String) options.valueOf("config");
		} else {
			configFileName = "config.properties";
		}

		List<String> forcedFiles = new ArrayList<>();
		if (options.has("force")) {
			List<String> tmp = Arrays.asList(((String)options.valueOf("force")).split(","));
			for( String s : tmp ) {
				forcedFiles.add(s.trim());
			}
		}
		
		List<Integer> steps = new ArrayList<>();
		if (options.has("steps")) {
			List<String> tmp = Arrays.asList(((String)options.valueOf("steps")).split(","));
			for( String s : tmp ) {
				steps.add(Integer.valueOf(s));
			}
		}
		
		// Loads the configuration file
		Properties config = new Properties();
		try {
			FileInputStream fis = new FileInputStream(new File(configFileName));
			config.load(fis);
			fis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Runs the converter
		DBConvertService service = new DBConvertService();
		try {
			service.doConvert(config, steps, forcedFiles);
		} catch ( Exception e ) {
			e.printStackTrace();
		}
		
	}
}
