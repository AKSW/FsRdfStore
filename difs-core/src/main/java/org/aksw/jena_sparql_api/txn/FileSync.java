package org.aksw.jena_sparql_api.txn;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.function.Consumer;

import org.aksw.commons.io.util.FileChannelUtils;


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
	
	public FileSync(
			Path targetFile, Path oldContentFile, Path newContentFile) {
		super();
		this.targetFile = targetFile;
		this.newContentFile = newContentFile;
		this.oldContentFile = oldContentFile;
		
		this.oldContentTmpFile = oldContentFile.resolveSibling(oldContentFile.getFileName().toString() + ".tmp");
		this.newContentTmpFile = newContentFile.resolveSibling(newContentFile.getFileName().toString() + ".tmp");
	}


	public static FileSync create(Path path) {
		String fileName = path.getFileName().toString();
		
		return new FileSync(
				path,
				path.resolveSibling(fileName + ".sync.old"),
				path.resolveSibling(fileName + ".sync.new"));
	}
		
//	public void createBackup() throws IOException {
//		Files.
//		Files.copy(targetFile, oldContentTmpFile, StandardCopyOption.REPLACE_EXISTING);
//	}
	
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
			moveAtomic(newContentTmpFile, newContentFile);
		}
	}

	public static void moveAtomic(Path srcFile, Path tgtPath) throws IOException {
		try {
			Files.move(srcFile, tgtPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
		} catch (AtomicMoveNotSupportedException e) {			
			try (
					FileChannel srcChannel = FileChannel.open(srcFile, StandardOpenOption.READ);
					FileChannel tgtChannel = FileChannel.open(tgtPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
				long n = srcChannel.size();
				FileChannelUtils.transferFromFully(tgtChannel, srcChannel, 0, n, null);
				tgtChannel.force(true);
			}
			
			Files.delete(srcFile);
		}		
	}
	

	public void recoverPreCommit() throws IOException {
		// If there is no newContent but a newContentTmp then we reached an inconsistent state:
		// We cannot commit if the newContent was not fully written
		boolean newContentFileExists = Files.exists(newContentFile);
		boolean neContentTmpFileExists = Files.exists(newContentTmpFile);
		
		if (!Files.exists(newContentFile) && Files.exists(newContentTmpFile)) {
			throw new IllegalStateException();
		}
		
		// If any new content file exists without backup something went wrong as well
		
		
		// If there is no backup of the existing data then create it
		if (!Files.exists(oldContentFile)) {
			moveAtomic(targetFile, oldContentFile);
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
					moveAtomic(targetFile, oldContentFile);
				}
			}

			// Move the tmp content to new content
			moveAtomic(newContentTmpFile, newContentFile);
		}
		
		
		// Move new new content to the target
		if (Files.exists(newContentFile)) {
			// TODO Skip update if newContentFile and targetFile are the same (have the same timestamp)
			moveAtomic(newContentFile, targetFile);
		}
	}
	
	@Override
	public void finalizeCommit() throws IOException {
		Files.deleteIfExists(oldContentTmpFile);	
		Files.deleteIfExists(oldContentFile);
	}
	
	
	@Override
	public void rollback() throws IOException {
		Files.deleteIfExists(newContentFile);
		Files.deleteIfExists(newContentTmpFile);
		
		if (Files.exists(oldContentFile)) {
			moveAtomic(oldContentFile, targetFile);
		}
	}
}
