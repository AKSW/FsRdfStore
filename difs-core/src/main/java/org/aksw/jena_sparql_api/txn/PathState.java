package org.aksw.jena_sparql_api.txn;

import java.nio.file.Path;
import java.time.Instant;

/**
 * A path with cached metadata. Currently this includes only the timestamp.
 * In the future this may include a content hash.
 *
 * @author raven
 *
 */
public class PathState {
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