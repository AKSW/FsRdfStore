package org.aksw.jena_sparql_api.difs.main;

import java.nio.file.Path;

public interface ResourceRepository<T> {

    Path getRootPath();
    String[] getPathSegments(T name);

}
