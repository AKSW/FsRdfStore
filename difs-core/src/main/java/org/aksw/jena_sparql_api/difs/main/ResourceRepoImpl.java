package org.aksw.jena_sparql_api.difs.main;

import java.nio.file.Path;

import org.aksw.commons.io.util.UriToPathUtils;
import org.aksw.jena_sparql_api.txn.ResourceRepository;

public class ResourceRepoImpl
	implements ResourceRepository<String>
{
	protected Path rootPath;
	
	public ResourceRepoImpl(Path rootPath) {
		super();
		this.rootPath = rootPath;
	}

	@Override
	public Path getRootPath() {
		return rootPath;
	}

	@Override
	public Path getRelPath(String name) {
		return UriToPathUtils.resolvePath(name);
	}
	
}
