package com.ciessa.museum.dbconvert.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
			service.doConvert(config);
		} catch ( Exception e ) {
			e.printStackTrace();
		}
		

	}
}
