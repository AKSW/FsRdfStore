package org.aksw.jena_sparql_api.txn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.aksw.commons.lambda.throwing.ThrowingConsumer;

public class FileUtilsX {

    public static void ensureParentFolderExists(Path childPath, ThrowingConsumer<Path> action) throws IOException {
        Path parentPath = childPath.getParent();

        if (parentPath == null) {
            try {
                action.accept(childPath);
            } catch (Exception e) {
                throw new IOException(e);
            }
        } else {
            ensureFolderExists(parentPath, tmpParentPath -> action.accept(childPath));
        }
    }

    /**
     * Repeatedly retry to create the given file's parent folder and then perform an action based on that folder.
     * Uses 10 retries with 100ms delay before failing.
     * Useful if another process concurrently perform a 'deleteEmptyFolders' action an the same folder
     *
     *
     * @param path
     * @param action
     * @throws IOException
     */
    public static void ensureFolderExists(Path folderPath, ThrowingConsumer<Path> action) throws IOException {
        // FIXME Use our retry util instead and return a retry action which can be reconfigured
        for (int i = 0; i < 10; ++i) {
            Files.createDirectories(folderPath);
            try {
                action.accept(folderPath);
                break;
            } catch (Exception e) {
                if (!Files.exists(folderPath)) {
                    // logger.debug("Retrying folder creation: " + folderPath);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                    continue;
                } else {
                    throw new IOException(e);
                }
            }
        }
    }

    /**
     * Delete a specific path and then - regardless of deletion outcom e -try to delete all empty directories up to a given baseFolder.
     * Empty folders are only deleted if their path starts with the baseFolder
     *
     * The result is the same as of {@link Files#deleteIfExists(Path)}.
     */
    public static boolean deleteFileIfExistsAndThenDeleteEmptyFolders(Path path, Path baseFolder) throws IOException {
        boolean result = Files.deleteIfExists(path);
        path = path.getParent();
        if (path != null) {
            deleteEmptyFolders(path, baseFolder);
        }

        return result;
    }

    /** Delete parent folders of 'path' that are descendants of baseFolder (inclusive) */
    public static void deleteEmptyFolders(Path path, Path baseFolder) {
        while (path.startsWith(baseFolder)) {
            if (Files.exists(path)) {
                if (!Files.isDirectory(path)) {
                    throw new IllegalArgumentException("Path must be a directory: " + path);
                }

                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    // Ignore
                    break;
                }
            }
            path = path.getParent();
        }
    }

}
