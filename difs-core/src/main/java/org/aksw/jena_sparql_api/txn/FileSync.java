package org.aksw.jena_sparql_api.txn;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.function.Consumer;

import org.aksw.commons.io.util.FileUtils;


/**
 * Protocol implementation to create/read/update/(delete) data with syncing to a file
 * in a transaction-safe way.
 *
 */
public class FileSync
    implements TxnComponent
{
    protected Path targetFile;
    protected Path newContentFile;
    protected Path newContentTmpFile;
    protected Path oldContentFile;
    protected Path oldContentTmpFile;

    /** Whether to delete 'targetFile' if the new content is empty */
    protected boolean deleteTargetFileOnUpdateWithEmptyContent;

    public FileSync(
            Path targetFile, Path oldContentFile, Path newContentFile, boolean deleteTargetFileOnUpdateWithEmptyContent) {
        super();
        this.targetFile = targetFile;
        this.newContentFile = newContentFile;
        this.oldContentFile = oldContentFile;

        this.oldContentTmpFile = oldContentFile.resolveSibling(oldContentFile.getFileName().toString() + ".tmp");
        this.newContentTmpFile = newContentFile.resolveSibling(newContentFile.getFileName().toString() + ".tmp");

        this.deleteTargetFileOnUpdateWithEmptyContent = deleteTargetFileOnUpdateWithEmptyContent;
    }


    public static FileSync create(Path path, boolean deleteTargetFileOnUpdateWithEmptyContent) {
        String fileName = path.getFileName().toString();

        return new FileSync(
                path,
                path.resolveSibling(fileName + ".sync.old"),
                path.resolveSibling(fileName + ".sync.new"),
                deleteTargetFileOnUpdateWithEmptyContent);
    }

//	public void createBackup() throws IOException {
//		Files.
//		Files.copy(targetFile, oldContentTmpFile, StandardCopyOption.REPLACE_EXISTING);
//	}

    public Path getTargetFile() {
        return targetFile;
    }

    public Path getOldContentPath() {
        Path result = Files.exists(oldContentFile)
            ? oldContentFile
            : targetFile
            ;
        return result;
    }

    public Path getCurrentPath() {
        Path result = Files.exists(newContentFile)
            ? newContentFile
            : targetFile
            ;

        return result;
    }

    public InputStream openCurrentContent() throws IOException {
        Path currentPath = getCurrentPath();
        InputStream result = Files.newInputStream(currentPath);
        return result;
    }

    public OutputStream newOutputStreamToNewTmpContent() throws IOException {
        Files.createDirectories(newContentTmpFile.getParent());
        OutputStream result = Files.newOutputStream(newContentTmpFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        return result;
    }

    public boolean exists() {
        Path currentPath = getCurrentPath();
        return Files.exists(currentPath);
    }

    public Instant getLastModifiedTime() throws IOException {
        Instant result;
        Path currentPath = getCurrentPath();
        try {
            FileTime ft = Files.getLastModifiedTime(currentPath);
            result = ft.toInstant();
        } catch (NoSuchFileException e) {
            result = null;
        }
        return result;
    }


    /**
     * Set the new content of a resource.
     * The new content is not committed.
     *
     * @param outputStreamSupplier
     * @throws IOException
     */
    public void putContent(Consumer<OutputStream> outputStreamSupplier) throws IOException {
        // Delete a possibly prior written newContentFile
        Files.deleteIfExists(newContentFile);
        try (OutputStream out = newOutputStreamToNewTmpContent()) {
            outputStreamSupplier.accept(out);
            FileUtils.moveAtomic(newContentTmpFile, newContentFile);
        }
    }

    public void recoverPreCommit() throws IOException {
        // If there is no newContent but a newContentTmp then we reached an inconsistent state:
        // We cannot commit if the newContent was not fully written
        boolean newContentFileExists = Files.exists(newContentFile);
        boolean newContentTmpFileExists = Files.exists(newContentTmpFile);

        if (!newContentFileExists && newContentTmpFileExists) {
            throw new IllegalStateException();
        }

        // If any new content file exists without backup something went wrong as well


        // If there is no backup of the existing data then create it
        if (!Files.exists(oldContentFile)) {
            FileUtils.moveAtomic(targetFile, oldContentFile);
        }
    }


    /**
     * Replace the new content with the current temporary content
     * @throws IOException
     */
    @Override
    public void preCommit() throws IOException {
        // If there is a pending change
        if (Files.exists(newContentTmpFile)) {
            // If there is no backup of the existing data then create it
            if (!Files.exists(oldContentFile)) {
                // If there is no prior file just create a 0 byte file
                if (!Files.exists(targetFile)) {
                    Files.createDirectories(oldContentFile.getParent());
                    Files.createFile(oldContentFile);
                } else {
                    FileUtils.moveAtomic(targetFile, oldContentFile);
                }
            }

            // Move the tmp content to new content
            FileUtils.moveAtomic(newContentTmpFile, newContentFile);
        }


        // Move new new content to the target
        if (Files.exists(newContentFile)) {
            // TODO Skip update if newContentFile and targetFile are the same (have the same timestamp)
            FileUtils.moveAtomic(newContentFile, targetFile);
        }
    }

    @Override
    public void finalizeCommit() throws IOException {
        Files.deleteIfExists(oldContentTmpFile);
        Files.deleteIfExists(oldContentFile);

        long size = Files.size(targetFile);
        if (size == 0 && deleteTargetFileOnUpdateWithEmptyContent) {
            Files.deleteIfExists(targetFile);
        }
    }


    @Override
    public void rollback() throws IOException {
        Files.deleteIfExists(newContentFile);
        Files.deleteIfExists(newContentTmpFile);

        if (Files.exists(oldContentFile)) {
            FileUtils.moveAtomic(oldContentFile, targetFile);
        }
    }
}
