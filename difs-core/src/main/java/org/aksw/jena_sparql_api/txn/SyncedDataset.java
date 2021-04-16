package org.aksw.jena_sparql_api.txn;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;

import org.aksw.jena_sparql_api.rx.DatasetGraphFactoryEx;
import org.aksw.jena_sparql_api.utils.SetFromDatasetGraph;
import org.aksw.jena_sparql_api.utils.model.DatasetGraphDiff;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;


/**
 * A path with cached metadata. Currently this includes only the timestamp.
 * In the future this may include a content hash.
 * 
 * @author raven
 *
 */
class PathState {
	protected Path path;
	protected Instant timestamp;

	public PathState(Path path, Instant timestamp) {
		super();
		this.path = path;
		this.timestamp = timestamp;
	}

	public Path getPath() {
		return path;
	}

	public void setPath(Path path) {
		this.path = path;
	}

	public Instant getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Instant timestamp) {
		this.timestamp = timestamp;
	}


	@Override
	public String toString() {
		return "PathState [path=" + path + ", timestamp=" + timestamp + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((path == null) ? 0 : path.hashCode());
		result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
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
		PathState other = (PathState) obj;
		if (path == null) {
			if (other.path != null)
				return false;
		} else if (!path.equals(other.path))
			return false;
		if (timestamp == null) {
			if (other.timestamp != null)
				return false;
		} else if (!timestamp.equals(other.timestamp))
			return false;
		return true;
	}
}



public class SyncedDataset {
	private static final Logger logger = LoggerFactory.getLogger(SyncedDataset.class);

	/**
	 * A class that holds the original and current state of path metadata.
	 * 
	 * @author raven
	 */
	public static class State {
		protected PathState originalState;
		protected PathState currentState;
		
		public State(PathState originalState, PathState currentState) {
			super();
			this.originalState = originalState;
			this.currentState = currentState;
		}

		public PathState getOriginalState() {
			return originalState;
		}

		public void setOriginalState(PathState originalState) {
			this.originalState = originalState;
		}

		public PathState getCurrentState() {
			return currentState;
		}

		public void setCurrentState(PathState currentState) {
			this.currentState = currentState;
		}
		
		@Override
		public String toString() {
			return "State [originalState=" + originalState + ", currentState=" + currentState + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((currentState == null) ? 0 : currentState.hashCode());
			result = prime * result + ((originalState == null) ? 0 : originalState.hashCode());
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
			if (currentState == null) {
				if (other.currentState != null)
					return false;
			} else if (!currentState.equals(other.currentState))
				return false;
			if (originalState == null) {
				if (other.originalState != null)
					return false;
			} else if (!originalState.equals(other.originalState))
				return false;
			return true;
		}
	}
	
	protected FileSync fileSync;
	
	protected State state;
	protected DatasetGraph originalState;
	protected DatasetGraphDiff diff = null;

	public SyncedDataset(FileSync fileSync) {
		super();
		this.fileSync = fileSync;
	}
	
	
	public static Instant getTimestamp(Path path) {
		Instant result = null;
		try {
			if (Files.exists(path)) {								
				FileTime timestamp = Files.getLastModifiedTime(path);
				if (timestamp == null) {
					timestamp = FileTime.fromMillis(0l);
				}
				
				result = timestamp.toInstant();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return result;
	}

	public State getState() {
		Path originalSourcePath = fileSync.getOldContentPath();
		Path diffSourcePath = fileSync.getCurrentPath();
		
		Instant originalTimestamp = getTimestamp(originalSourcePath);
		Instant diffTimestamp = diffSourcePath == originalSourcePath
				? originalTimestamp
				: getTimestamp(diffSourcePath);

		State result = new State(
			new PathState(originalSourcePath, originalTimestamp),
			new PathState(diffSourcePath, diffTimestamp)
		);

		logger.debug("Loaded state: " + result);
		
		return result;
	}
	
	public void updateState() {
		this.state = getState();
	}
	
	public void forceLoad() {
		state = getState();

		originalState = DatasetGraphFactoryEx.createInsertOrderPreservingDatasetGraph();

		try (InputStream in = Files.newInputStream(state.getCurrentState().getPath())) {
			RDFDataMgr.read(originalState, in, Lang.TRIG);
		} catch (AccessDeniedException ex) {
			// FIXME The file may not exist but it may also be an authorization issue
			logger.warn("Access denied: " + ExceptionUtils.getRootCauseMessage(ex));
		} catch (NoSuchFileException ex) {
			// Ignore
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}

		if (!state.getCurrentState().getPath().equals(state.getOriginalState().getPath())) {
			DatasetGraph n = DatasetGraphFactoryEx.createInsertOrderPreservingDatasetGraph();
			try (InputStream in = Files.newInputStream(state.getCurrentState().getPath())) {
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
	
	public DatasetGraphDiff getDiff() {
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
		ensureLoaded();
		return diff;
	}
	
	/**
	 * Returns true if there are pending changes in memory; i.e. the set of added/removed triples is non-empty.
	 * 
	 * @return
	 */
	public boolean isDirty() {
		boolean result = diff != null && !(diff.getAdded().isEmpty() && diff.getRemoved().isEmpty());
		return result;
	}
	
	
	
	public void ensureUpToDate() {
		Objects.requireNonNull(state);
		
		// Check the time stamps of the source resources
		State verify = getState();

		if (!verify.equals(state)) {
			throw new RuntimeException(
				String.format("Content of files was changed externally since it was loaded:\nExpected:\n%s: %s\n%s: %s\nActual:\n%s: %s\n%s: %s",
				state.getOriginalState().getPath(),
				state.getOriginalState().getTimestamp(),
				state.getCurrentState().getPath(),
				state.getCurrentState().getTimestamp(),
				verify.getOriginalState().getPath(),
				verify.getOriginalState().getTimestamp(),
				verify.getCurrentState().getPath(),
				verify.getCurrentState().getTimestamp()
			));
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
				
				// Update metadata
				updateState();
				
				
				
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}	
}
