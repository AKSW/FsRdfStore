package org.aksw.jena_sparql_api.txn;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import org.aksw.commons.io.util.FileChannelUtils;

public class FileSync {
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
	}


	public FileSync create(Path path) {
		String fileName = path.getFileName().toString();
		
		return new FileSync(
				path,
				path.resolveSibling(fileName + ".sync.old"),
				path.resolve(fileName + ".sync.new"));
	}
		
//	public void createBackup() throws IOException {
//		Files.
//		Files.copy(targetFile, oldContentTmpFile, StandardCopyOption.REPLACE_EXISTING);
//	}
	
	public OutputStream newOutputStreamToNewTmpContent() throws IOException {
		OutputStream result = Files.newOutputStream(newContentTmpFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
		return result;
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
	
	/**
	 * Replace the new content with the current temporary content
	 * @throws IOException 
	 */
	public void precommit() throws IOException {
		// Backup the existing data
		moveAtomic(targetFile, oldContentFile);

		// Move the tmp content to new content
		moveAtomic(newContentTmpFile, newContentFile);
		
		// Move new new content to the target
		moveAtomic(newContentFile, targetFile);
	}
	
	public void commit() throws IOException {
		Files.delete(oldContentTmpFile);	
		Files.delete(oldContentFile);
	}
	
	
	public void rollback() throws IOException {
		Files.delete(newContentFile);
		Files.delete(newContentTmpFile);
		moveAtomic(oldContentFile, targetFile);
	}
}
