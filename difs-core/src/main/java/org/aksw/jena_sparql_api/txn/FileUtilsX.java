package org.aksw.jena_sparql_api.txn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileUtilsX {
	
	/** Delete a path and then try to delete all empty directories up to a given baseFolder.
	 * Empty folders are only deleted if their path starts with the baseFolder */ 
	public static void deleteFileIfExistsAndThenDeleteEmptyFolders(Path path, Path baseFolder) throws IOException {
		Files.deleteIfExists(path);
		path = path.getParent();
		deleteEmptyFolders(path, baseFolder);
	}

	public static void deleteEmptyFolders(Path path, Path baseFolder) {
		if (!Files.isDirectory(path)) {
			throw new IllegalArgumentException("Path must be a directory: " + path);
		}

		while (path.startsWith(baseFolder)) {
			try {
				Files.deleteIfExists(path);
			} catch (IOException e) {
				// Ignore
				break;
			}
			
			path = path.getParent();
		}
	}

}
