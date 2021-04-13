package org.aksw.difs.example.main;

import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.webdav.WebdavFileSystemConfigBuilder;

import com.sshtools.vfs2nio.Vfs2NioFileSystemProvider;


public class MainTestWebdav {
	
	public static void main(String[] args) throws Exception {
		/* The actual Commons VFS URI to access */  
		String vfsUri = "webdav://localhost/webdav";
		
//		Map<String, Object> webdavOpts = new HashMap<>();
//		webdavOpts.put("followRedirect", true);

//		Map<String, Object> env = new HashMap<>();
//		env.put(Vfs2NioFileSystemProvider.FILE_SYSTEM_OPTIONS, webdavOpts);
		
		FileSystemOptions web = new FileSystemOptions();
		// WebdavFileSystemConfigBuilder.getInstance().setFollowRedirect(web, true);

		Map<String, Object> env = new HashMap<>();
		env.put(Vfs2NioFileSystemProvider.FILE_SYSTEM_OPTIONS, web);

		FileSystem fs = FileSystems.newFileSystem(
				URI.create("vfs:" + vfsUri), 
				env);
		
		for(Path p : fs.getRootDirectories()) {
			p = p.resolve("webdav");
			System.out.println(p);
			try(DirectoryStream<Path> d = Files.newDirectoryStream(p)) {
				for(Path dp : d) {
					System.out.println("  " + dp);	
				}
			}
		}
		
		
//		System.out.println(Paths.get("/test"));
//		System.out.println(Paths.get("/test/"));
//		
//		
//		if (true) {
//			return;
//		}
//		
//		Sardine sardine = SardineFactory.begin();
//		List<DavResource> resources = sardine.list("http://localhost/webdav/gitalog");
//		for (DavResource res : resources)
//		{
////			res.isDirectory()
////			res.
//		     System.out.println(res);
//		}
//		// sardineFactory.
//		
////		String userInfo = "user:password";
//		String userInfo = " : ";
//        URI uri = new URI("webdav", userInfo,"localhost", 80, "/webdav", null, null);
//
//        FileSystem fs = FileSystems.newFileSystem(uri, null);
//        Path root = fs.getPath("/");
//        
//        try (Stream<Path> stream = Files.list(root)) {
//        	stream.forEach(path -> {
//        		System.out.println(path);
//        	});
//        }
//        
	}

}
