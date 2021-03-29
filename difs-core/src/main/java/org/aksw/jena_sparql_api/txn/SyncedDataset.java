package org.aksw.jena_sparql_api.txn;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;

import org.aksw.jena_sparql_api.rx.DatasetGraphFactoryEx;
import org.aksw.jena_sparql_api.utils.SetFromDatasetGraph;
import org.aksw.jena_sparql_api.utils.model.DatasetGraphDiff;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;

import com.google.common.collect.Sets;


public class SyncedDataset {
	
	public static class State {
		protected Path originalSourcePath;
		protected Instant originalTimestamp;
		
		protected Path diffSourcePath;
		protected Instant diffTimetamp;
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((diffSourcePath == null) ? 0 : diffSourcePath.hashCode());
			result = prime * result + ((diffTimetamp == null) ? 0 : diffTimetamp.hashCode());
			result = prime * result + ((originalSourcePath == null) ? 0 : originalSourcePath.hashCode());
			result = prime * result + ((originalTimestamp == null) ? 0 : originalTimestamp.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			State other = (State) obj;
			if (diffSourcePath == null) {
				if (other.diffSourcePath != null)
					return false;
			} else if (!diffSourcePath.equals(other.diffSourcePath))
				return false;
			if (diffTimetamp == null) {
				if (other.diffTimetamp != null)
					return false;
			} else if (!diffTimetamp.equals(other.diffTimetamp))
				return false;
			if (originalSourcePath == null) {
				if (other.originalSourcePath != null)
					return false;
			} else if (!originalSourcePath.equals(other.originalSourcePath))
				return false;
			if (originalTimestamp == null) {
				if (other.originalTimestamp != null)
					return false;
			} else if (!originalTimestamp.equals(other.originalTimestamp))
				return false;
			return true;
		}
	}
	
	protected FileSync fileSync;
	
	protected State state;
	protected DatasetGraph originalState;
	protected DatasetGraphDiff diff;

	public SyncedDataset(FileSync fileSync) {
		super();
		this.fileSync = fileSync;
	}
	
	
	public static Instant getTimestamp(Path path) {
		Instant result = null;
		try {
			if (Files.exists(path)) {
				result = Files.getLastModifiedTime(path).toInstant();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return result;
	}

	public State getState() {
		State result = new State();
		result.originalSourcePath = fileSync.getOldContentPath();
		result.originalTimestamp = getTimestamp(result.originalSourcePath);

		result.diffSourcePath = fileSync.getCurrentPath();
		result.diffTimetamp = result.diffSourcePath == result.originalSourcePath
				? result.originalTimestamp
				: getTimestamp(result.diffSourcePath);
		
		return result;
	}
	
	public void forceLoad() {
		state = getState();

		originalState = DatasetGraphFactoryEx.createInsertOrderPreservingDatasetGraph();
		
		
		try (InputStream in = Files.newInputStream(state.originalSourcePath)) {
			RDFDataMgr.read(originalState, in, Lang.TRIG);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}

		if (!state.diffSourcePath.equals(state.originalSourcePath)) {
			DatasetGraph n = DatasetGraphFactoryEx.createInsertOrderPreservingDatasetGraph();
			try (InputStream in = Files.newInputStream(state.diffSourcePath)) {
				RDFDataMgr.read(n, in, Lang.TRIG);
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}

			Set<Quad> oldQuads = SetFromDatasetGraph.wrap(originalState);
			Set<Quad> newQuads = SetFromDatasetGraph.wrap(n);
			
			Set<Quad> addedQuads = Sets.difference(newQuads, oldQuads);
			Set<Quad> removedQuads = Sets.difference(oldQuads, newQuads);

			diff = new DatasetGraphDiff(originalState);
			addedQuads.forEach(diff.getAdded()::add);
			removedQuads.forEach(diff.getRemoved()::add);
		} else {
			diff = new DatasetGraphDiff(originalState);
		}
	}
	
	public void ensureLoaded() {
		if (originalState == null) {
			forceLoad();
		}
	}
	
	public DatasetGraph getOriginalState() {
		ensureLoaded();
		return originalState;
	}
	
	public DatasetGraph getCurrentState() {
		ensureLoaded();
		return diff;
	}
	
	public DatasetGraph getAdditions() {
		return diff.getAdded();
	}

	public DatasetGraph getDeletions() {
		return diff.getRemoved();
	}

	public FileSync getEntity() {
		return fileSync;
	}
	
	public void load() {
		ensureLoaded();
	}
	
	public DatasetGraph get() {
		return diff;
	}
	
	public boolean isDirty() {
		boolean result = !(diff.getAdded().isEmpty() && diff.getRemoved().isEmpty());
		return result;
	}
	
	
	
	public void ensureUpToDate() {
		Objects.requireNonNull(state);
		
		// Check the time stamps of the source resources
		State verify = getState();
		
		if (verify.equals(state)) {
			throw new RuntimeException("There were changes");
		}
	}
	
//	public Synced set(T instance) {
//		this.instance = instance;
//	}
	
	public void save() {
		if (isDirty()) {
			try {
				ensureUpToDate();
				
				fileSync.putContent(out -> {
					RDFDataMgr.write(out, diff, RDFFormat.TRIG_PRETTY);
				});
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}	
}
