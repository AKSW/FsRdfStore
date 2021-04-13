package org.aksw.jena_sparql_api.difs.main;

import java.nio.file.Path;
import java.util.function.Function;

import org.aksw.commons.io.util.UriToPathUtils;
import org.aksw.commons.util.strings.StringUtils;
import org.aksw.jena_sparql_api.txn.ResourceRepository;

public class ResourceRepoImpl
	implements ResourceRepository<String>
{
	protected Path rootPath;
	protected Function<String, String[]> resToPath;
	
	public ResourceRepoImpl(Path rootPath, Function<String, String[]> resToRelPath) {
		super();
		this.rootPath = rootPath;
		this.resToPath = resToRelPath;
	}

	@Override
	public Path getRootPath() {
		return rootPath;
	}

	@Override
	public String[] getPathSegments(String name) {
		String[] result = resToPath.apply(name);
		return result;
	}

	public static ResourceRepository<String> createWithUriToPath(Path rootPath) {
		return new ResourceRepoImpl(rootPath, UriToPathUtils::toPathSegments);
	}

	/** Create file names by means of urlencoding and prepending a dot ('.') */
	public static ResourceRepository<String> createWithUrlEncode(Path rootPath) {
		return new ResourceRepoImpl(rootPath, ResourceRepoImpl::stringToPath);
	}
	
	public static String[] stringToPath(String name) {
		String str = StringUtils.urlEncode(name);
		if (str.length() > 64) {
			str = StringUtils.md5Hash(str);
		}
		
		// Path r = Paths.get(str);
		return new String[] {str};
	}
}
