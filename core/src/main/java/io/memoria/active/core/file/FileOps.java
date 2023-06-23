package io.memoria.active.core.file;

import io.vavr.collection.List;
import io.vavr.control.Try;

import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

public class FileOps {

  private FileOps() {}

  // -------------------------------------------------------------------------
  // Create
  // -------------------------------------------------------------------------
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public static void createDir(Path path) {
    path.toFile().mkdirs();
  }

  public static Try<Path> rewrite(Path path, String content) {
    return Try.of(() -> Files.createDirectories(path.getParent()))
              .flatMap(p -> Try.of(() -> Files.writeString(path, content, TRUNCATE_EXISTING, CREATE)));
  }

  public static Try<Path> write(Path path, String content) {
    return Try.of(() -> Files.createDirectories(path.getParent()))
              .flatMap(p -> Try.of(() -> Files.writeString(path, content, CREATE_NEW)));
  }

  // -------------------------------------------------------------------------
  // Read
  // -------------------------------------------------------------------------

  public static Try<List<Path>> listAll(Path path) {
    return Try.of(() -> Files.list(path)).map(List::ofAll);
  }

  /**
   * @return list of all files in a path
   */
  public static Try<List<Path>> listFiles(Path path) {
    return listAll(path).map(paths -> paths.filter(f -> !Files.isDirectory(f)).sorted());
  }

  /**
   * @return list of directories inside a path
   */
  public static Try<List<Path>> listDir(Path path) {
    return listAll(path).map(paths -> paths.filter(Files::isDirectory).sorted());
  }

  public static Try<String> read(Path path) {
    return Try.of(() -> Files.readString(path));
  }

  public static Try<List<String>> readAsLines(Path path) {
    return Try.of(() -> Files.lines(path)).map(List::ofAll);
  }

  public static Try<Path> lastModifiedFile(Path path) {
    return listFiles(path).map(paths -> paths.reduce(FileOps::lastModifiedFile));
  }

  public static Path lastModifiedFile(Path p1, Path p2) {
    return (p1.toFile().lastModified() > p2.toFile().lastModified()) ? p1 : p2;
  }

  // -------------------------------------------------------------------------
  // Delete
  // -------------------------------------------------------------------------

  /**
   * @return deleted file path
   */
  public static Try<Path> deleteFile(Path path) {
    return Try.of(() -> Files.deleteIfExists(path)).map(i -> path);
  }

  /**
   * @return deleted files in a path
   */
  public static Try<List<Path>> deleteDirFiles(Path path) {
    if (Files.exists(path))
      return listFiles(path).map(paths -> paths.flatMap(FileOps::deleteFile));
    else
      return Try.success(List.empty());
  }

  public static Try<Path> deleteDir(Path path) {
    if (Files.exists(path)) {
      deleteDirFiles(path);
      listDir(path).forEach(subDirTry -> subDirTry.forEach(FileOps::deleteDir));
      return Try.of(() -> {
        Files.delete(path);
        return path;
      });
    } else {
      return Try.success(path);
    }
  }
}
