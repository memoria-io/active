package io.memoria.active.core.file;

import io.vavr.collection.List;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class FileOpsTest {
  private static final Path TEST_DIR = Path.of("/tmp/rFilesTest");
  private static final Path TEST_DIR_FILE_TXT = TEST_DIR.resolve("file.txt");

  @BeforeEach
  void beforeEach() {
    FileOps.deleteDir(TEST_DIR).get();
    FileOps.createDir(TEST_DIR);
  }

  @AfterEach
  void afterEach() {
    FileOps.deleteDir(TEST_DIR).get();
  }

  @Test
  void folderExists() {
    assert Files.exists(TEST_DIR);
  }

  @Test
  @DisplayName("Should create parent dirs and write in file, then rewrite again")
  void createParents() throws IOException {
    // Given
    var filePath = TEST_DIR.resolve("childDir").resolve("grandChildDir").resolve("file.txt");
    // When
    assertThat(FileOps.write(filePath, "hello world").get()).isEqualTo(filePath);
    // Then
    assertThat(new String(Files.readAllBytes(filePath))).isEqualTo("hello world");
    // And when
    assertThat(FileOps.rewrite(filePath, "hello world again").get()).isEqualTo(filePath);
    // Then
    assertThat(new String(Files.readAllBytes(filePath))).isEqualTo("hello world again");
  }

  @Test
  void deleteOneFile() throws IOException {
    // Given
    FileOps.createDir(TEST_DIR);
    Files.createFile(TEST_DIR_FILE_TXT);
    // When
    var deleteFile = FileOps.deleteFile(TEST_DIR_FILE_TXT).get();
    // Then
    assertThat(deleteFile).isEqualTo(TEST_DIR_FILE_TXT);
  }

  @Test
  void deleteDirectoryFiles() throws IOException {
    // Given
    FileOps.createDir(TEST_DIR);
    Files.createFile(TEST_DIR.resolve("File01.txt"));
    Files.createFile(TEST_DIR.resolve("File02.txt"));
    // When
    var deleteFile = FileOps.deleteDirFiles(TEST_DIR).get();
    // Then
    assertThat(deleteFile.size()).isEqualTo(2);
    assertThat(FileOps.listFiles(TEST_DIR).get().size()).isZero();
  }

  @Test
  @DisplayName("Delete directory and all sub directories and files")
  void deleteDirFilesAndSubDirs() {
    // Given
    var nDirs = 3;
    var nFiles = 5;
    // When
    var createdFiles = List.range(0, nDirs)
                           .map(i -> TEST_DIR.resolve(String.valueOf(i)))
                           .flatMap(dir -> createSomeFiles(dir, nFiles));

    // then
    assertThat(createdFiles.size()).isEqualTo(nDirs * nFiles);
    assertThat(FileOps.listDir(TEST_DIR).get().size()).isEqualTo(nDirs);
    assert FileOps.deleteDir(TEST_DIR).isSuccess();
  }

  @Test
  void lastModifiedFile() {
    // Given
    int nFiles = 10;
    var lastFileName = nFiles - 1 + ".json";
    createSomeFiles(TEST_DIR, nFiles);

    // When
    var lastModifiedFile = FileOps.lastModifiedFile(TEST_DIR).get();
    // Then
    assertThat(lastModifiedFile).isEqualTo(TEST_DIR.resolve(lastFileName));
  }

  @Test
  void list() {
    // Given
    FileOps.createDir(TEST_DIR);
    var listFlux = FileOps.listFiles(TEST_DIR).get();
    // Then
    assertThat(listFlux).isEmpty();
  }

  @Test
  void read() throws IOException {
    // Given
    FileOps.createDir(TEST_DIR);
    Files.writeString(TEST_DIR_FILE_TXT, "welcome");
    // When
    var read = FileOps.read(TEST_DIR_FILE_TXT).get();
    // Then
    assertThat(read).isEqualTo("welcome");
  }

  @Test
  void readLines() throws IOException {
    // Given
    FileOps.createDir(TEST_DIR);
    Files.writeString(TEST_DIR_FILE_TXT, "welcome\nhello");
    // When
    var read = FileOps.readAsLines(TEST_DIR_FILE_TXT).get();
    // Then
    assertThat(read.toJavaList()).asList().hasSameElementsAs(List.of("welcome", "hello"));
  }

  @Test
  @DisplayName("Should create a new file")
  void rewrite() throws IOException {
    // When
    assertThat(FileOps.rewrite(TEST_DIR_FILE_TXT, "hello world").get()).isEqualTo(TEST_DIR_FILE_TXT);
    assertThat(FileOps.rewrite(TEST_DIR_FILE_TXT, "hi world").get()).isEqualTo(TEST_DIR_FILE_TXT);
    // Then
    var str = new String(Files.readAllBytes(TEST_DIR_FILE_TXT));
    assertThat(str).isEqualTo("hi world");
  }

  @Test
  @DisplayName("Should create a new file")
  void write() throws IOException {
    // When
    assertThat(FileOps.write(TEST_DIR_FILE_TXT, "hello world").get()).isEqualTo(TEST_DIR_FILE_TXT);
    // Then
    var str = new String(Files.readAllBytes(TEST_DIR_FILE_TXT));
    assertThat(str).isEqualTo("hello world");
  }

  private List<Path> createSomeFiles(Path path, int count) {
    return List.range(0, count).map(i -> FileOps.write(path.resolve(i + ".json"), "hi" + i).get());
  }
}
