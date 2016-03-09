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
package org.icgc.dcc.repository.ega.submission;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.TransportException;
import org.icgc.dcc.repository.ega.model.EGAAnalysisFile;
import org.icgc.dcc.repository.ega.model.EGAStudyFile;
import org.icgc.dcc.repository.ega.model.EGASubmissionFile;
import org.json.XML;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class EGASubmissionReader {

  /**
   * Constants.
   */
  private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JsonOrgModule());

  private static final Pattern STUDY_FILE_PATTERN = Pattern.compile(""
      // study/study.[study].xml
      // e.g. study/study.PCAWG.xml
      + "study/study\\."
      + "([^.]+)" // [study]
      + "\\.xml");
  private static final Pattern ANALYSIS_FILE_PATTERN = Pattern.compile(""
      // [projectId]/analysis_[type].[study]_[workflow]/analysis/analysis.[analysisId].xml
      // e.g. BRCA-UK/analysis_alignment.PCAWG_WGS_BWA/analysis/analysis.4acd08c6-1354-414d-8961-1f04acb2275c.xml
      + "([^/]+)" // [projectId]
      + "/analysis_"
      + "([^.]+)" // [type]
      + "\\."
      + "([^_]+)" // [study]
      + "_"
      + "([^/]+)" // [workflow]
      + "/analysis/analysis\\."
      + "([^.]+)" // [analysisId]
      + "\\.xml");

  /**
   * Configuration.
   */
  @NonNull
  private final String repoUrl;
  private final File repoDir = new File("/tmp/dcc-repository-ega");

  @SneakyThrows
  public List<EGASubmissionFile> readSubmissionFiles() {
    updateRepo();

    val studyFiles = readStudyFiles(repoDir);
    val analysisFiles = readAnalysisFiles(repoDir);
    val submissionFiles = createSubmissionFiles(studyFiles, analysisFiles);

    return submissionFiles;
  }

  private List<EGASubmissionFile> createSubmissionFiles(List<EGAStudyFile> studyFiles,
      List<EGAAnalysisFile> analysisFiles) {
    val studyIndex = Maps.<String, EGAStudyFile> uniqueIndex(studyFiles, f -> f.getStudy());
    return analysisFiles.stream().map(f -> new EGASubmissionFile(studyIndex.get(f.getStudy()), f))
        .collect(toImmutableList());
  }

  private void updateRepo() throws GitAPIException, InvalidRemoteException, TransportException, IOException {
    if (repoDir.exists()) {
      log.info("Pulling '{}' in '{}'...", repoUrl, repoDir);
      Git
          .open(repoDir)
          .pull();
    } else {
      checkState(repoDir.mkdirs(), "Could not create '%s'", repoDir);

      log.info("Cloning '{}' to '{}'...", repoUrl, repoDir);
      Git
          .cloneRepository()
          .setURI(repoUrl)
          .setDirectory(repoDir)
          .call();
    }
  }

  @SneakyThrows
  private List<EGAStudyFile> readStudyFiles(File repoDir) {
    return Files
        .walk(repoDir.toPath())
        .filter(this::isXmlFile)
        .filter(this::isStudyFile)
        .map(this::createStudyFile)
        .collect(toImmutableList());
  }

  @SneakyThrows
  private List<EGAAnalysisFile> readAnalysisFiles(File repoDir) {
    return Files
        .walk(repoDir.toPath())
        .filter(this::isXmlFile)
        .filter(this::isAnalysisFile)
        .map(this::createAnalysisFile)
        .collect(toImmutableList());
  }

  private boolean isXmlFile(Path path) {
    return path.toFile().getName().endsWith(".xml");
  }

  private boolean isStudyFile(Path path) {
    return matchStudyFile(path).matches();
  }

  private boolean isAnalysisFile(Path path) {
    return matchAnalysisFile(path).matches();
  }

  private Matcher matchStudyFile(Path path) {
    return matchFile(path, STUDY_FILE_PATTERN);
  }

  private Matcher matchAnalysisFile(Path path) {
    return matchFile(path, ANALYSIS_FILE_PATTERN);
  }

  private Matcher matchFile(Path path, Pattern pattern) {
    val relativePath = repoDir.toPath().relativize(path);
    return pattern.matcher(relativePath.toString());
  }

  private EGAStudyFile createStudyFile(Path path) {
    val matcher = matchStudyFile(path);
    checkState(matcher.find());

    return new EGAStudyFile(
        matcher.group(1),
        readFile(path));
  }

  private EGAAnalysisFile createAnalysisFile(Path path) {
    val matcher = matchAnalysisFile(path);
    checkState(matcher.find());

    return new EGAAnalysisFile(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        matcher.group(4),
        matcher.group(5),
        readFile(path));
  }

  @SneakyThrows
  private static ObjectNode readFile(Path path) {
    val xml = Resources.toString(path.toUri().toURL(), UTF_8);
    val json = XML.toJSONObject(xml);
    return MAPPER.convertValue(json, ObjectNode.class);
  }

}
