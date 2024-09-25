package appledog.stream.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class PropertiesFileReader {
    private PropertiesFileReader(){}
    public static Properties readConfig(String path) {
        Properties prop = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get(path))) {
            prop.load(input);
            return prop;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return prop;
    }
}
