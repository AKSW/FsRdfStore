

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;


/**
 * Symlink strategy that creates ordinary files whose content is the string
 * representation of the target path being linked to.
 * 
 * @author raven
 *
 */
public class SymlinkStrategyFile
	implements SymlinkStrategy
{
	@Override
	public void createSymbolicLink(Path link, Path target) throws IOException {
		byte[] bytes = target.toString().getBytes(StandardCharsets.UTF_8);
		try (FileChannel fc = FileChannel.open(link, StandardOpenOption.CREATE_NEW, StandardOpenOption.DSYNC)) {
			fc.write(ByteBuffer.wrap(bytes));
			fc.force(true); // Is that needed if we use DSYNC?
		}
	}

	@Override
	public Path readSymbolicLink(Path link) throws IOException {
		byte[] bytes = Files.readAllBytes(link);
		String str = new String(bytes, StandardCharsets.UTF_8);
		Path result = Paths.get(str);
		return result;
	}

}
