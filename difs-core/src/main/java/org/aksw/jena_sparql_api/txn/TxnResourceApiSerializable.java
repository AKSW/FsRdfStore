package org.aksw.jena_sparql_api.txn;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Instant;

import org.aksw.commons.io.util.PathUtils;
import org.aksw.jena_sparql_api.lock.db.api.LockOwner;
import org.aksw.jena_sparql_api.lock.db.api.ResourceLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Api to a resource w.r.t. a transaction.
 * 
 * 
 * @author raven
 *
 */
public class TxnResourceApiSerializable
	extends TxnResourceApiReadUncommitted<TxnSerializable>
{
	private static final Logger logger = LoggerFactory.getLogger(TxnResourceApiSerializable.class);

	protected Path journalEntryFile;


	protected ResourceLock<String> resourceLock;
	protected LockOwner txnResourceLock;

	//		public ResourceApi(String resourceName) {
		//this.resourceName = resourceName;
	public TxnResourceApiSerializable(TxnSerializable txn, String[] resKey) {// Path resFilePath) {
		super(txn, resKey);

		String resKeyStr = PathUtils.join(resKey);

		resourceLock = txn.txnMgr.lockStore.getLockForResource(resKeyStr);
		txnResourceLock = resourceLock.get(txn.txnId);
		
		
		String[] resLockKey = txn.txnMgr.lockRepo.getPathSegments(resKeyStr);
		String resLockKeyStr = PathUtils.join(resLockKey);
		
		
//		resShadowPath = txnMgr.resShadow.getRelPath(resourceName);			
//		resFilename = StringUtils.urlEncode(resourceName);

		journalEntryFile = txn.txnFolder.resolve("." + resLockKeyStr);
	}
	
	@Override
	public LockOwner getTxnResourceLock() {
		return txnResourceLock;
	}

	@Override
	public boolean isVisible() {
		boolean result;
		
		if (txnResourceLock.isLockedHere()) {
			result = true;
		} else {				
			Instant txnTime = txn.getCreationInstant();
			Instant resTime;
			try {
				resTime = fileSync.getLastModifiedTime();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

			// If the resource's modified time is null then it did not exist yet
			result = resTime != null && resTime.isBefore(txnTime); 
		}

		return result;
	}
	
	@Override
	public void declareAccess() {
		// Path actualLinkTarget = txnFolder.relativize(resShadowAbsPath);
		Path actualLinkTarget = txn.txnFolder.relativize(resFileAbsPath);
		try {
			if (Files.exists(journalEntryFile, LinkOption.NOFOLLOW_LINKS)) {
				boolean verifyAccess = false;
				
				if (verifyAccess) {
					logger.debug("Verifying access " + journalEntryFile);
					// Verify
					Path link = txn.txnMgr.symlinkStrategy.readSymbolicLink(journalEntryFile);
					if (!link.equals(actualLinkTarget)) {
						throw new RuntimeException(String.format("Validation failed: Attempted to declare access to %s but a different %s already existed ", actualLinkTarget, link));
					}
				}					
			} else {
				logger.debug("Declaring access from " + journalEntryFile + " to " + actualLinkTarget);
				FileUtilsX.ensureParentFolderExists(journalEntryFile, f -> {
					try {
						txn.txnMgr.symlinkStrategy.createSymbolicLink(journalEntryFile, actualLinkTarget);
					} catch (FileAlreadyExistsException e) {
						// Ignore
						// TODO Verify whether the existing symlink matches the one we wanted to write?
					}
				});
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}			
	}

	@Override
	public void undeclareAccess() {
		try {
			// TODO Use delete instead and log an exception?
			Files.deleteIfExists(journalEntryFile);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}			
	}
}