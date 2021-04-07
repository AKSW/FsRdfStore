

import java.io.IOException;
import java.nio.file.Path;

public interface SymlinkStrategy {
	void createSymbolicLink(Path link, Path target) throws IOException;
	Path readSymbolicLink(Path link) throws IOException;
}
