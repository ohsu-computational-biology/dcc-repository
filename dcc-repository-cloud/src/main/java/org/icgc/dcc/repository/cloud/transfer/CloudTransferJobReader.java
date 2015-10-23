/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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

import static com.google.common.io.Files.createTempDir;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.TransportException;
import org.icgc.dcc.common.core.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

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
  private final Collection<String> paths;

  @SneakyThrows
  public List<ObjectNode> readJobs() {
    val repoDir = cloneRepo();
    val completedDirs = resolveCompletedDirs(repoDir);

    val jobs = ImmutableList.<ObjectNode> builder();
    for (val completedDir : completedDirs) {
      log.info("Resolving job files from completed dir '{}'...", completedDir.getCanonicalPath());
      val jobFiles = resolveJobFiles(completedDir);

      log.info("Reading {} completed jobs from {}...", formatCount(jobFiles.size()), completedDir);
      jobs.addAll(readFiles(jobFiles));
    }

    return jobs.build();
  }

  private List<ObjectNode> readFiles(List<Path> files) throws IOException, JsonProcessingException {
    return files.stream().map(this::readFile).collect(toImmutableList());
  }

  @SneakyThrows
  private ObjectNode readFile(Path jsonFile) {
    log.debug("Reading '{}'...", jsonFile);
    return (ObjectNode) MAPPER.readTree(jsonFile.toFile());
  }

  private File cloneRepo() throws GitAPIException, InvalidRemoteException, TransportException {
    val repoDir = createTempDir();
    log.info("Cloning '{}' to '{}'...", repoUrl, repoDir);
    Git
        .cloneRepository()
        .setURI(repoUrl)
        .setDirectory(repoDir)
        .call();

    return repoDir;
  }

  @SneakyThrows
  private static List<Path> resolveJobFiles(File completedDir) {
    return Files.list(completedDir.toPath()).filter(isJsonFile()).collect(toImmutableList());
  }

  private List<File> resolveCompletedDirs(File repoDir) {
    return paths.stream().map(path -> new File(repoDir, path)).collect(Collectors.toImmutableList());
  }

  private static Predicate<? super Path> isJsonFile() {
    return path -> path.toString().endsWith(".json");
  }

}
