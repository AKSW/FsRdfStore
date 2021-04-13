package org.aksw.jena_sparql_api.dataset.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aksw.common.io.util.symlink.SymbolicLinkStrategy;
import org.aksw.commons.io.util.PathUtils;
import org.aksw.commons.io.util.SymLinkUtils;
import org.aksw.commons.io.util.UriToPathUtils;
import org.aksw.commons.util.strings.StringUtils;
import org.aksw.jena_sparql_api.txn.ResourceRepository;
import org.apache.jena.ext.com.google.common.io.MoreFiles;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.util.NodeUtils;

import com.google.common.collect.Streams;

public class DatasetGraphIndexerFromFileSystem
    implements DatasetGraphIndexPlugin
{
	protected SymbolicLinkStrategy symlinkStrategy;

    protected Path basePath;
//    protected Path propertyFolder;
    protected Node propertyNode;
    protected Function<? super Node, String[]> objectToPath;
    protected Function<String, Path> uriToPath;
    
    /** The file the index point to - TODO this should be obtained from some store object */
    protected String tgtFilename = "data.trig";
    
    // We need that graph in order to re-use its mapping
    // from (subject) iris to paths
    // protected DatasetGraphFromFileSystem syncedGraph;
    protected ResourceRepository<String> syncedGraph;

    // Extract the symlink strategy from the dataset graph
//    public static SymlinkStrategy extractSymlinkStrategy(DatasetGraph dg) {
//    	DatasetGraphFromTxnMgr tdg = (DatasetGraphFromTxnMgr)dg;
//    	TxnMgr txnMgr = tdg.getTxnMgr();
//    	SymlinkStrategy result = txnMgr.getSymlinkStrategy();
//    	return result;    	
//    }
    
    public DatasetGraphIndexerFromFileSystem(
    		SymbolicLinkStrategy symlinkStrategy,
    		ResourceRepository<String> syncedGraph,
            Node propertyNode,
            Path basePath,
            Function<Node, String[]> objectToPath) {
        super();
        this.symlinkStrategy = symlinkStrategy;
        this.basePath = basePath;
        this.propertyNode = propertyNode;
        this.syncedGraph = syncedGraph;
        this.objectToPath = objectToPath;
    }

    public static String[] uriNodeToPath(Node node) {
        String[] result = node.isURI()
                ? UriToPathUtils.toPathSegments(node.getURI())
                : null;

        return result;
    }


    public static String[] iriOrLexicalFormToToPath(Node node) {
    	String[] result = node.isLiteral()
    			? new String[] {StringUtils.urlEncode(node.getLiteralLexicalForm())}
    			: node.isURI()
    				? UriToPathUtils.toPathSegments(node.getURI())
    				: node.isBlank()
    					? PathUtils.splitBySlash(node.getBlankNodeLabel())
    					: null;
    	return result;
    }

    public static Path mavenStringToToPath(Node node) {
        String str = node.isURI() ? node.getURI() : NodeUtils.stringLiteral(node);

        // Remove trailing slashes and
        // FIXME Replace '..' only if it would be interpreted as moving up the directory
        Path result = null;
        if (str != null) {
            String[] parts = str.replace(':', '/').replaceAll("^/*", "").split("/");
            String tmp = Arrays.asList(parts).stream()
                .filter(part -> !part.equals(".."))
                .map(StringUtils::urlEncode)
                .collect(Collectors.joining("/"));

            result = Paths.get(tmp);
        }

        return result;
    }


    public static String pathToFilename(String[] relPath) {
    	String result = PathUtils.join(relPath).replace("/", ".");
    	if (result.length() > 64) {
    		result = StringUtils.md5Hash(result);
    	}
    	return result;
    }
    
    @Override
    public void add(DatasetGraph dg, Node g, Node s, Node p, Node o) {
//        Node g = quad.getGraph();
//        Node s = quad.getSubject();
//        Node p = quad.getPredicate();
//        Node o = quad.getObject();
        if (evaluateFind(s, p, o) != null) {
            String[] idxRelPath = objectToPath.apply(o);


//            Path idxRelPath = UriToPathUtils.resolvePath(oRelPath);
            Path idxFullPath = PathUtils.resolve(basePath, idxRelPath);

            String tgtIri = g.getURI();
            Path tgtBasePath = syncedGraph.getRootPath();
            String[] tgtRelPath = syncedGraph.getPathSegments(tgtIri);

            String coreName = pathToFilename(tgtRelPath);
            
            // String tgtFileName = syncedGraph.getFilename();
            // String tgtFileName = filename;
            Path symLinkTgtAbsFile = PathUtils.resolve(tgtBasePath, tgtRelPath).resolve(tgtFilename);
//            Path symLinkTgtRelFile = idxFullPath.relativize(symLinkTgtAbsFile);

            Path file = Paths.get(tgtFilename);
            String prefix = MoreFiles.getNameWithoutExtension(file);
            prefix = prefix + "." + coreName;
            String suffix = MoreFiles.getFileExtension(file);
            suffix = suffix.isEmpty() ? "" : "." + suffix;
            suffix += ".link";

            try {
                Files.createDirectories(idxFullPath);
                // TODO Possibly extend allocateSymbolicLink with a flag to update the symlink rather
                // having to catch FileAlreadyExistsException here
                //SymlinkStrategy symlinkStrategy = extractSymlinkStrategy(dg);
                SymLinkUtils.allocateSymbolicLink(symlinkStrategy, symLinkTgtAbsFile, idxFullPath, prefix, suffix);
            } catch (Exception e) {
                 throw new RuntimeException(e);
            }
        }
    }


    /**
     * Delete a quad from the index.
     * 
     */
    @Override
    public void delete(DatasetGraph dg, Node g, Node s, Node p, Node o) {
//        Node g = quad.getGraph();
//        Node s = quad.getSubject();
//        Node p = quad.getPredicate();
//        Node o = quad.getObject();
        if (evaluateFind(s, p, o) != null) {
//            String idxIri = o.getURI();
//            Path idxRelPath = UriToPathUtils.resolvePath(idxIri);
            String[] idxRelPath = objectToPath.apply(o);
            
            Quad deleteQuad = new Quad(g, s, p, o);
            
            // Check whether any other quad in the same graph wrt. dg maps to the same idxRelPath - if so do not delete the symlink
            long count = Streams.stream(dg.find(g, Node.ANY, p, Node.ANY))
            	.filter(q -> !q.equals(deleteQuad))
            	.filter(q -> {
            		String[] otherRelPath = objectToPath.apply(q.getObject());
            		return otherRelPath.equals(idxRelPath);
            	})
            	.count();
            
            if (count == 0) {
	            
	            Path idxFullPath = PathUtils.resolve(basePath, idxRelPath);
	
	// Should we sanity check that the symlink refers to the exact same target
	// as it would if we created it from the quad?
	//            String tgtIri = g.getURI();
	//            Path tgtRelPath = syncedGraph.getRelPathForIri(tgtIri);
	//            String tgtFileName = syncedGraph.getFilename();
	            String tgtIri = g.getURI();
	            Path tgtBasePath = syncedGraph.getRootPath();
	            String[] tgtRelPath = syncedGraph.getPathSegments(tgtIri);

	            String coreName = pathToFilename(tgtRelPath);

//	             String tgtFileName = filename;

	
//	            Path file = Paths.get(tgtFilename);
//	            String prefix = preprefix + MoreFiles.getNameWithoutExtension(file);
//	            String suffix = MoreFiles.getFileExtension(file);
//	            suffix = suffix.isEmpty() ? "" : "." + suffix;

	            // FIXME This looks broken - delete won't work
	            Path symLinkSrcFile = idxFullPath.resolve(tgtFilename);
	            
	//            Path symLinkTgtFile = tgtRelPath.resolve(tgtFileName);
	
	
	            try {
	                boolean isSymlink = Files.isSymbolicLink(symLinkSrcFile);
	                if (isSymlink) {
	                    Files.delete(symLinkSrcFile);
	                    // TODO Delete empty directory
	                    // FileUtils.deleteDirectoryIfEmpty(basePath, symLinkSrcFile.getParent());
	                }
	            } catch (Exception e) {
	                throw new RuntimeException(e);
	            }
	        }
	    }
    }

//    protected void onPreCommit(DatasetGraphDiff diff) {
//        diff.getRemoved().find(Node.ANY, Node.ANY, propertyNode, Node.ANY)
//            .forEachRemaining(this::processRemoval);
//
//        diff.getAdded().find(Node.ANY, Node.ANY, propertyNode, Node.ANY)
//            .forEachRemaining(this::processAddition);
//    }
//

    public Float evaluateFind(Node s, Node p, Node o) {
        Float result = propertyNode.equals(p) && (o != null && objectToPath.apply(o) != null)
                ? Float.valueOf(1.0f)
                : null;
        return result;
    }

    
    /**
     * The resulting stream must be closed in order to avoid 'too many open files' errors!
     * 
     * @param sourceFolder
     * @param prefix
     * @param suffix
     * @return
     * @throws IOException
     */
    public static Stream<Entry<Path, Path>> readSymbolicLinks(SymbolicLinkStrategy symlinkStrategy, Path sourceFolder, String prefix, String suffix) throws IOException {
        return Files.list(sourceFolder)
                .filter(symlinkStrategy::isSymbolicLink)
                .filter(path -> {
                    String fileName = path.getFileName().toString();

                    boolean r = fileName.startsWith(prefix) && fileName.endsWith(suffix);
                    // TODO Check that the string between prefix and suffix is either an empty string
                    // or corresponds to a number
                    return r;
                })
                .flatMap(path -> {
                    Stream<Entry<Path, Path>> r;
                    try {
                        r = Stream.of(new SimpleEntry<>(path, symlinkStrategy.readSymbolicLink(path)));
                    } catch (IOException e) {
                        // logger.warn("Error reading symoblic link; skipping", e);
                        r = Stream.empty();
                    }
                    return r;
                });
    }
    
    public Stream<String[]> listGraphNodes(DatasetGraph dg, Node s, Node p, Node o) {
//    	SymlinkStrategy symlinkStrategy = extractSymlinkStrategy(dg);
    	
        if (evaluateFind(s, p, o) == null) {
            throw new RuntimeException("Index is not suitable for lookups with predicate " + p);
        }

//        String iri = o.getURI();
//        Path relPath = UriToPathUtils.resolvePath(iri);
        String[] relPath = objectToPath.apply(o);
        //Path relPath = syncedGraph.getRelPathForIri(tgtIri);
//        String fileName = syncedGraph.getFilename();

        String coreName = pathToFilename(relPath);

        Path file = Paths.get(tgtFilename);
        String prefix = MoreFiles.getNameWithoutExtension(file);
        prefix = prefix + "." + coreName;
        String suffix = MoreFiles.getFileExtension(file);
        suffix = suffix.isEmpty() ? "" : "." + suffix;
        suffix += ".link";

//        Path symLinkTgtFile = relPath.resolve(fileName);


        Path symLinkSrcPath = PathUtils.resolve(basePath, relPath);
        Stream<Entry<Path, Path>> symLinkTgtPaths;
        try {
            symLinkTgtPaths = Files.exists(symLinkSrcPath)
                    ? readSymbolicLinks(symlinkStrategy, symLinkSrcPath, prefix, suffix)
                    : Stream.empty();
            
            
            Stream<String[]> result = symLinkTgtPaths.map(srcToTgt -> {
                Path absTgt = SymLinkUtils.resolveSymLinkAbsolute(srcToTgt.getKey(), srcToTgt.getValue());
                Path tgtRelFile = syncedGraph.getRootPath().relativize(absTgt);

                // Get the path (without the filename)
                Path tgtRelPath = tgtRelFile.getParent();
                return tgtRelPath;
            })
            .map(PathUtils::getPathSegments);
            
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Load all graphs that are linked to and call find on the union
    }

//    public Iterator<Node> listGraphNodes(Node s, Node p, Node o) {
//        if (evaluateFind(s, p, o) == null) {
//            throw new RuntimeException("Index is not suitable for lookups with predicate " + p);
//        }
//
////        String iri = o.getURI();
////        Path relPath = UriToPathUtils.resolvePath(iri);
//        Path relPath = objectToPath.apply(o);
//        //Path relPath = syncedGraph.getRelPathForIri(tgtIri);
////        String fileName = syncedGraph.getFilename();
//
//        Path file = Paths.get(filename);
//        String prefix = MoreFiles.getNameWithoutExtension(file);
//        String suffix = MoreFiles.getFileExtension(file);
//        suffix = suffix.isEmpty() ? "" : "." + suffix;
//
////        Path symLinkTgtFile = relPath.resolve(fileName);
//
//
//        Path symLinkSrcPath = basePath.resolve(relPath);
//        Map<Path, Path> symLinkTgtPaths;
//        try {
//            symLinkTgtPaths = Files.exists(symLinkSrcPath)
//                    ? SymLinkUtils.readSymbolicLinks(symLinkSrcPath, prefix, suffix)
//                    : Collections.emptyMap();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//
//        // Load all graphs that are linked to and call find on the union
//
//        IteratorConcat<Node> result = new IteratorConcat<>();
//        for (Entry<Path, Path> srcToTgt : symLinkTgtPaths.entrySet()) {
////            Path absSymLinkTgt = srcToTgt.getValue();
////            Path tgtRelPath;
////            try {
//                Path absTgt = SymLinkUtils.resolveSymLinkAbsolute(srcToTgt.getKey(), srcToTgt.getValue());
//                Path tgtRelFile = syncedGraph.getRootPath().relativize(absTgt);
//
//                // Get the path (without the filename)
//                Path tgtRelPath = tgtRelFile.getParent();
////            } catch (IOException e1) {
////                throw new RuntimeException(e1);
////            }
//            Entry<Path, Dataset> e = syncedGraph.getOrCreate(tgtRelPath);
//
//            Iterator<Node> it = e.getValue().asDatasetGraph().listGraphNodes();
//            result.add(it);
//        }
//
//        return result;
////        CatalogResolverFilesystem.allocateSymbolicLink(rawTarget, rawSourceFolder, baseName)
//    }

}
