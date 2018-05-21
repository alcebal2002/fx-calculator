package utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneralUtils {

	// Logger
	private static Logger logger = LoggerFactory.getLogger(GeneralUtils.class);
	
	public static final Date getDateFromString(final String date, final String format) {
		Date result = null;
		try {
			// format example: "dd-mm-yyyy"
			result = new SimpleDateFormat(format).parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return result;
	}

	public static boolean checkIfFileExists (final String currentCurrency) {

		boolean exists = false;

		String historicalDataPath = ApplicationProperties.getStringProperty("main.historicalDataPath");
		String historicalDataFileExtension = ApplicationProperties.getStringProperty("main.historicalDataFileExtension");

		String file = historicalDataPath + currentCurrency + historicalDataFileExtension;

		File f = new File(file);
		if(f.exists() && !f.isDirectory()) {
			exists = true;
		}

		return exists;

	}

	public static List<String> getFilesFromPath (final String path, final String extension) {
		List<String> filesList = new ArrayList<String>();
		File dir = new File(path);
		for (File file : dir.listFiles()) {
			if (file.getName().toLowerCase().endsWith((extension))) {
				filesList.add(file.getName());
			}
		}
		return filesList;
	}
	
	public static String printElapsedTime (final long startTime, final long endTime) {
		
		long millis = endTime - startTime;
		return (printElapsedTime(millis));
	}
	
	public static String printElapsedTime (final long totalMillis) {
		
		long millis = totalMillis;
		
		long days = TimeUnit.MILLISECONDS.toDays(millis);
		millis -= TimeUnit.DAYS.toMillis(days); 
		long hours = TimeUnit.MILLISECONDS.toHours(millis);
		millis -= TimeUnit.HOURS.toMillis(hours);
		long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
		millis -= TimeUnit.MINUTES.toMillis(minutes); 
		long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);

		return (millis + " ms - (" + hours + " hrs " + minutes + " min " + seconds + " secs)");
	}

	
	// Wirte text to file 
	public static void writeTextToFile (final Path path, final String text) {

		BufferedWriter bWriter = null;

		try {
		    Files.write(path, Arrays.asList(text), StandardCharsets.UTF_8,
		        Files.exists(path) ? StandardOpenOption.APPEND : StandardOpenOption.CREATE);
		} catch (Exception e) {
			logger.error ("Exception while writing the results to file: " + e.getClass() + " - " + e.getMessage());
		} finally {
			try {
				if (bWriter != null)
					bWriter.close();
			} catch (IOException ex) {
				logger.error ("Exception while closing the results file: " + ex.getClass() + " - " + ex.getMessage());
			}
		}

	}
}
