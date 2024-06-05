
package utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class TextFileReader {
	public static List<String> readLines(String filePath) {
		return readLines(new File(filePath));
	}
	
	public static List<String> readLines(File file) {
		List<String> fileContents = new ArrayList<>();
		try {
			Scanner scanner = new Scanner(file);
			while (scanner.hasNextLine()) {
				fileContents.add(scanner.nextLine());
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return fileContents;
	}
}