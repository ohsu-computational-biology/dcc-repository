/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.repository.cloud.transfer;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.eclipse.jgit.api.errors.GitAPIException;
import org.icgc.dcc.repository.core.util.TransferMetadataRepository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CloudTransferJobReader {

  /**
   * Constants.
   */
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Configuration.
   */
  @NonNull
  private final String repoUrl;
  @NonNull
  private final File repoDir;
  @NonNull
  private final String repoDirGlob;

  @SneakyThrows
  public List<ObjectNode> readJobs() {
    // Ensure we are in-sync with the remote
    updateMetadata();

    // Read and assemble
    return readFiles();
  }

  private void updateMetadata() throws GitAPIException, IOException {
    val repository = new TransferMetadataRepository(repoUrl, repoDir);
    repository.update();
  }

  private List<ObjectNode> readFiles() {
    return resolveCompletedDirs()
        .flatMap(this::resolveJobFiles)
        .map(this::readFile)
        .collect(toImmutableList());
  }

  @SneakyThrows
  private ObjectNode readFile(Path jsonFile) {
    log.debug("Reading '{}'...", jsonFile);
    return (ObjectNode) MAPPER.readTree(jsonFile.toFile());
  }

  @SneakyThrows
  private Stream<Path> resolveJobFiles(File completedDir) {
    log.info("Resolving job files from completed dir '{}'...", completedDir.getCanonicalPath());
    return Files.list(completedDir.toPath()).filter(isJsonFile());
  }

  @SneakyThrows
  private Stream<File> resolveCompletedDirs() {
    log.info("Resolving repo dirs using glob: '{}'", repoDirGlob);
    val dirs = Files.newDirectoryStream(repoDir.toPath(), repoDirGlob);
    return stream(dirs).map(d -> new File(d.toFile(), "completed-jobs"));
  }

  private static Predicate<? super Path> isJsonFile() {
    return path -> path.toFile().getName().endsWith(".json");
  }

}
