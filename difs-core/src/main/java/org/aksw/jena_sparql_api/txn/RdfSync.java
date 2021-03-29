package org.aksw.jena_sparql_api.txn;

import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;

import org.aksw.jena_sparql_api.rx.DatasetGraphFactoryEx;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.DatasetGraph;

//public class RdfSync {
//	/**
//	 * 
//	 * @param path The folder for the resource (not the data file itself)
//	 * @return
//	 */
//	public static Synced<FileSync, DatasetGraph> create(Path path) {
//		return create(FileSync.create(path.resolve("data.trig")));
//	}
//
//	public static Synced<FileSync, DatasetGraph> create(FileSync fileSync) {
//		return new Synced<FileSync, DatasetGraph>(
//				fileSync,
//				e -> {
//					DatasetGraph dg = DatasetGraphFactoryEx.createInsertOrderPreservingDatasetGraph();
//					if (e.exists()) {
//						try (InputStream in = e.openCurrentContent()) {
//							RDFDataMgr.read(dg, in, Lang.TRIG);
//						} catch (Exception ex) {
//							throw new RuntimeException(ex);
//						}
//					}
//					return dg;
//				},
//				(e, dg) -> {
//					try {
//						fileSync.putContent(out -> {
//							RDFDataMgr.write(out, dg, RDFFormat.TRIG_PRETTY);
//						});
//					} catch (Exception ex) {
//						throw new RuntimeException(ex);
//					}
//				},
//				e -> {
//					try {
//						Instant r = e.exists() ? e.getLastModifiedTime() : null;
//						return r;
//					} catch (Exception ex) {
//						throw new RuntimeException(ex);
//					}
//				}
//			);
//	}
//}
