package org.aksw.jena_sparql_api.txn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.aksw.jena_sparql_api.lock.LockManager;

public class TxnMgr {
	protected LockManager<Path> lockMgr;
	protected Path txnBasePath;
	protected ResourceRepository<String> resRepo;
	protected ResourceRepository<String> resShadow;
	
	public TxnMgr(
			LockManager<Path> lockMgr,
			Path txnBasePath,
			ResourceRepository<String> resRepo,
			ResourceRepository<String> resShadow) {
		super();
		this.lockMgr = lockMgr;
		this.txnBasePath = txnBasePath;
		this.resRepo = resRepo;
		this.resShadow = resShadow;
	}
	
	public ResourceRepository<String> getResRepo() {
		return resRepo;
	}

	public TxnImpl newTxn(boolean isWrite) {
		String txnId = "txn-" + new Random().nextLong();

		Path txnFolder = txnBasePath.resolve(txnId);

		try {
			Files.createDirectories(txnFolder);

			if (isWrite) {
				Files.createFile(txnFolder.resolve("write"));
			}
		} catch (IOException e) {
			throw new RuntimeException("Failed to lock txn folder");
		}
		
		return new TxnImpl(this, txnId, txnFolder);
	}
}
