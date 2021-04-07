package org.aksw.difs.example.main;

import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import com.github.sardine.DavResource;
import com.github.sardine.Sardine;
import com.github.sardine.SardineFactory;

public class MainTestWebdav {
	
	public static void main(String[] args) throws Exception {
		Sardine sardine = SardineFactory.begin();
		List<DavResource> resources = sardine.list("http://localhost/webdav/gitalog");
		for (DavResource res : resources)
		{
//			res.isDirectory()
//			res.
		     System.out.println(res);
		}
		// sardineFactory.
		
//		String userInfo = "user:password";
		String userInfo = " : ";
        URI uri = new URI("webdav", userInfo,"localhost", 80, "/webdav", null, null);

        FileSystem fs = FileSystems.newFileSystem(uri, null);
        Path root = fs.getPath("/");
        
        try (Stream<Path> stream = Files.list(root)) {
        	stream.forEach(path -> {
        		System.out.println(path);
        	});
        }
        
	}

}
