package org.aksw.jena_sparql_api.txn;

import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;

import org.aksw.jena_sparql_api.rx.DatasetGraphFactoryEx;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.DatasetGraph;

public class RdfSync {
	public static Synced<FileSync, DatasetGraph> create(Path path) {
		return create(FileSync.create(path));
	}

	public static Synced<FileSync, DatasetGraph> create(FileSync fileSync) {
		return new Synced<FileSync, DatasetGraph>(
				fileSync,
				e -> {
					try (InputStream in = e.openCurrentContent()) {
						DatasetGraph dg = DatasetGraphFactoryEx.createInsertOrderPreservingDatasetGraph();
						RDFDataMgr.read(dg, in, Lang.TRIG);
						return dg;
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				},
				(e, dg) -> {
					try {
						fileSync.putContent(out -> {
							RDFDataMgr.write(out, dg, RDFFormat.TRIG_PRETTY);
						});
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				},
				e -> {
					try {
						Instant r = e.getLastModifiedTime();
						return r;
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			);
	}
}
