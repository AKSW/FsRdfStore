package org.aksw.jena_sparql_api.difs.main;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

import org.aksw.commons.io.util.UriToPathUtils;
import org.aksw.commons.util.strings.StringUtils;
import org.aksw.jena_sparql_api.txn.ResourceRepository;

public class ResourceRepoImpl
	implements ResourceRepository<String>
{
	protected Path rootPath;
	protected Function<String, Path> resToPath;
	
	public ResourceRepoImpl(Path rootPath, Function<String, Path> resToRelPath) {
		super();
		this.rootPath = rootPath;
		this.resToPath = resToRelPath;
	}

	@Override
	public Path getRootPath() {
		return rootPath;
	}

	@Override
	public Path getRelPath(String name) {
		Path result = resToPath.apply(name);
		return result;
	}

	public static ResourceRepository<String> createWithUriToPath(Path rootPath) {
		return new ResourceRepoImpl(rootPath, UriToPathUtils::resolvePath);
	}

	/** Create file names by means of urlencoding and prepending a dot ('.') */
	public static ResourceRepository<String> createWithUrlEncode(Path rootPath) {
		return new ResourceRepoImpl(rootPath, name -> Paths.get("." + StringUtils.urlEncode(name)));
	}

}
