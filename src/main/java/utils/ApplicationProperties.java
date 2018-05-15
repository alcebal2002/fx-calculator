package utils;

import static java.lang.System.out;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ApplicationProperties {

    private static Properties applicationProperties;
    private static String propertiesFile = "application.properties";

    public static String getPropertiesFile () {
    	return propertiesFile;
    }
    
    public static List<String> getListProperty (final String propertyName) {
        String values = getStringProperty (propertyName);
    	
    	return Arrays.asList(values.split("\\s*,\\s*"));
    }

    public static String getStringProperty (final String propertyName) {
        return getProperty(propertyName);
    }

    public static int getIntProperty (final String propertyName) {
        return Integer.parseInt(getProperty(propertyName));
    }

    public static float getFloatProperty (final String propertyName) {
        return Float.parseFloat(getProperty(propertyName));
    }

    public static boolean getBooleanProperty (final String propertyName) {
        return Boolean.parseBoolean(getProperty(propertyName));
    }

    public static String getProperty (final String propertyName) {

        if (applicationProperties == null) {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();

            try(InputStream resourceStream = loader.getResourceAsStream(propertiesFile)){
                applicationProperties = new Properties();
                applicationProperties.load(resourceStream);
            } catch (Exception ex) {
                out.println ("Exception: " + ex.getClass() + " - " + ex.getMessage());
            }
        }
        return applicationProperties.getProperty(propertyName);
    }
}