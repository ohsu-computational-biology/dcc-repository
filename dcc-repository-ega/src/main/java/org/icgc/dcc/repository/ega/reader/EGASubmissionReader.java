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
package org.icgc.dcc.repository.ega.reader;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getFirst;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.icgc.dcc.common.core.util.function.Predicates.not;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.repository.ega.model.EGAAnalysisFile.analysisFile;
import static org.icgc.dcc.repository.ega.model.EGAGnosFile.gnosFile;
import static org.icgc.dcc.repository.ega.model.EGAReceiptFile.receiptFile;
import static org.icgc.dcc.repository.ega.model.EGASampleFile.sampleFile;
import static org.icgc.dcc.repository.ega.model.EGAStudyFile.studyFile;
import static org.icgc.dcc.repository.ega.model.EGASubmission.submission;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.TransportException;
import org.icgc.dcc.repository.ega.model.EGAAnalysisFile;
import org.icgc.dcc.repository.ega.model.EGAGnosFile;
import org.icgc.dcc.repository.ega.model.EGAReceiptFile;
import org.icgc.dcc.repository.ega.model.EGASampleFile;
import org.icgc.dcc.repository.ega.model.EGAStudyFile;
import org.icgc.dcc.repository.ega.model.EGASubmission;
import org.json.XML;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.TreeMultimap;
import com.google.common.io.CharStreams;

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

  private static final Pattern TEST_FILE_PATTERN = Pattern.compile(""
      + "TEST-PROJ"
      + ".*");

  private static final Pattern STUDY_FILE_PATTERN = Pattern.compile(""
      // Template: study/study.[study].xml
      // Example : study/study.PCAWG.xml
      + "study/study"
      + "\\."
      + "([^.]+)" // [study]
      + "\\.xml");

  private static final Pattern SAMPLE_FILE_PATTERN = Pattern.compile(""
      // Template: [projectId]/sample/sample.[projectId].[type]_[timestamp].xml`
      // Example : BRCA-EU/sample/sample.BRCA-EU.wgs_1455048989.xml
      + "([^/]+)" // [projectId]
      + "/sample/sample"
      + "\\."
      + "[^.]+"
      + "\\."
      + "([^.]+)" // [type]
      + "_"
      + "([^.]+)" // [timestamp]
      + "\\.xml");

  private static final Pattern GNOS_FILE_PATTERN = Pattern.compile(""
      // Template: [projectId]/analysis_[type].[study]_[workflow]/GNOS_xml/analysis.[analysisId].GNOS.xml.gz
      // Example :
      // BRCA-EU/analysis_alignment.PCAWG_WGS_BWA/GNOS_xml/analysis.01b8baf1-9926-4118-9f4c-c2986bbfe561.GNOS.xml.gz
      + "([^/]+)" // [projectId]
      + "/analysis_"
      + "([^.]+)" // [type]
      + "\\."
      + "([^_]+)" // [study]
      + "_"
      + "([^/]+)" // [workflow]
      + "/GNOS_xml/analysis"
      + "\\."
      + "([^.]+)" // [analysisId]
      + "\\.GNOS\\.xml\\.gz");

  private static final Pattern ANALYSIS_FILE_PATTERN = Pattern.compile(""
      // Template: [projectId]/analysis_[type].[study]_[workflow]/analysis/analysis.[analysisId].xml
      // Example : BRCA-UK/analysis_alignment.PCAWG_WGS_BWA/analysis/analysis.4acd08c6-1354-414d-8961-1f04acb2275c.xml
      + "([^/]+)" // [projectId]
      + "/analysis_"
      + "([^.]+)" // [type]
      + "\\."
      + "([^_]+)" // [study]
      + "_"
      + "([^/]+)" // [workflow]
      + "/analysis/analysis"
      + "\\."
      + "([^.]+)" // [analysisId]
      + "\\.xml");

  private static final Pattern RECEIPT_FILE_PATTERN = Pattern.compile(""
      // Template:
      // [projectId]/analysis_[type].[study]_[workflow]/analysis/analysis.[analysisId].submission-[timestamp]_[id].xml
      // Example :
      // LICA-FR/analysis_alignment.PCAWG_WGS_BWA/analysis/analysis.4884bd78-4002-4379-89f5-5855454ff858.submission-1455301216_2e9ffc2d-d824-449a-bb2f-b313f8fda985.xml
      + "([^/]+)" // [projectId]
      + "/analysis_"
      + "([^.]+)" // [type]
      + "\\."
      + "([^_]+)" // [study]
      + "_"
      + "([^/]+)" // [workflow]
      + "/analysis/analysis"
      + "\\."
      + "([^.]+)" // [analysisId]
      + "\\."
      + "submission-"
      + "(\\d+)" // [timestamp]
      + "_[^.]+" // [id]
      + "\\.xml");

  /**
   * Configuration.
   */
  @NonNull
  private final String repoUrl;
  @NonNull
  private final File repoDir;

  @SneakyThrows
  public List<EGASubmission> readSubmissions() {
    // Ensure we are in-sync with the remote
    updateLocalRepo();

    // Read and assemble
    val studyFiles = readStudyFiles();
    val sampleFiles = readSampleFiles();
    val gnosFiles = readGnosFiles();
    val analysisFiles = readAnalysisFiles();
    val receiptFiles = readReceiptFiles();
    val submissions = createSubmissions(studyFiles, sampleFiles, gnosFiles, analysisFiles, receiptFiles);

    return submissions;
  }

  private List<EGASubmission> createSubmissions(
      List<EGAStudyFile> studyFiles,
      List<EGASampleFile> sampleFiles,
      List<EGAGnosFile> gnosFiles,
      List<EGAAnalysisFile> analysisFiles,
      List<EGAReceiptFile> receiptFiles) {

    // Index for lookup
    val studyIndex = Maps.<String, EGAStudyFile> uniqueIndex(studyFiles, EGAStudyFile::getStudy);
    val gnosIndex = Maps.<String, EGAGnosFile> uniqueIndex(gnosFiles, EGAGnosFile::getAnalysisId);

    val receiptIndex = TreeMultimap.<String, EGAReceiptFile> create();
    receiptFiles.forEach(f -> receiptIndex.put(f.getAnalysisId(), f));

    val sampleIndex = HashMultimap.<String, EGASampleFile> create();
    sampleFiles.forEach(f -> sampleIndex.put(f.getProjectId(), f));

    // Combine both files into a wrapper
    return analysisFiles.stream()
        .map(f -> submission()
            .studyFile(studyIndex.get(f.getStudy()))
            .sampleFiles(sampleIndex.get(f.getProjectId()))
            .gnosFile(gnosIndex.get(f.getAnalysisId()))
            .receiptFile(getFirst(receiptIndex.get(f.getAnalysisId()), null))
            .analysisFile(f)
            .build())
        .collect(toImmutableList());
  }

  private void updateLocalRepo() throws GitAPIException, InvalidRemoteException, TransportException, IOException {
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

  private List<EGAStudyFile> readStudyFiles() {
    return traverseRepo()
        .filter(this::isStudyFile)
        .map(this::createStudyFile)
        .collect(toImmutableList());
  }

  private List<EGASampleFile> readSampleFiles() {
    return traverseRepo()
        .filter(this::isSampleFile)
        .map(this::createSampleFile)
        .collect(toImmutableList());
  }

  private List<EGAGnosFile> readGnosFiles() {
    return traverseRepo()
        .filter(this::isGnosFile)
        .map(this::createGnosFile)
        .collect(toImmutableList());
  }

  private List<EGAAnalysisFile> readAnalysisFiles() {
    return traverseRepo()
        .filter(this::isAnalysisFile)
        .map(this::createAnalysisFile)
        .collect(toImmutableList());
  }

  private List<EGAReceiptFile> readReceiptFiles() {
    return traverseRepo()
        .filter(this::isReceiptFile)
        .map(this::createReceiptFile)
        .collect(toImmutableList());
  }

  @SneakyThrows
  private Stream<Path> traverseRepo() {
    return Files
        .walk(repoDir.toPath())
        .filter(not(this::isTestFile));
  }

  private boolean isTestFile(Path path) {
    return matchFile(path, TEST_FILE_PATTERN).matches();
  }

  private boolean isStudyFile(Path path) {
    return matchStudyFile(path).matches();
  }

  private boolean isSampleFile(Path path) {
    return matchSampleFile(path).matches();
  }

  private boolean isGnosFile(Path path) {
    return matchGnosFile(path).matches();
  }

  private boolean isAnalysisFile(Path path) {
    return matchAnalysisFile(path).matches();
  }

  private boolean isReceiptFile(Path path) {
    return matchReceiptFile(path).matches();
  }

  private Matcher matchStudyFile(Path path) {
    return matchFile(path, STUDY_FILE_PATTERN);
  }

  private Matcher matchSampleFile(Path path) {
    return matchFile(path, SAMPLE_FILE_PATTERN);
  }

  private Matcher matchGnosFile(Path path) {
    return matchFile(path, GNOS_FILE_PATTERN);
  }

  private Matcher matchAnalysisFile(Path path) {
    return matchFile(path, ANALYSIS_FILE_PATTERN);
  }

  private Matcher matchReceiptFile(Path path) {
    return matchFile(path, RECEIPT_FILE_PATTERN);
  }

  private Matcher matchFile(Path path, Pattern pattern) {
    // Match without using the absolute portion of the path
    val relativePath = repoDir.toPath().relativize(path);
    return pattern.matcher(relativePath.toString());
  }

  private EGAStudyFile createStudyFile(Path path) {
    // Parse template
    val matcher = matchStudyFile(path);
    checkState(matcher.find());

    // Combine path metadata with file metadata
    return studyFile()
        .study(matcher.group(1))
        .contents(readFile(path))
        .build();
  }

  private EGASampleFile createSampleFile(Path path) {
    // Parse template
    val matcher = matchSampleFile(path);
    checkState(matcher.find());

    // Combine path metadata with file metadata
    return sampleFile()
        .projectId(matcher.group(1))
        .type(matcher.group(2))
        .timestamp(Long.parseLong(matcher.group(3)))
        .contents(readFile(path))
        .build();
  }

  private EGAGnosFile createGnosFile(Path path) {
    // Parse template
    val matcher = matchGnosFile(path);
    checkState(matcher.find());

    // Combine path metadata with file metadata
    return gnosFile()
        .projectId(matcher.group(1))
        .type(matcher.group(2))
        .study(matcher.group(3))
        .workflow(matcher.group(4))
        .analysisId(matcher.group(5))
        .contents(readFile(path))
        .build();
  }

  private EGAAnalysisFile createAnalysisFile(Path path) {
    // Parse template
    val matcher = matchAnalysisFile(path);
    checkState(matcher.find());

    // Combine path metadata with file metadata
    return analysisFile()
        .projectId(matcher.group(1))
        .type(matcher.group(2))
        .study(matcher.group(3))
        .workflow(matcher.group(4))
        .analysisId(matcher.group(5))
        .contents(readFile(path))
        .build();
  }

  private EGAReceiptFile createReceiptFile(Path path) {
    // Parse template
    val matcher = matchReceiptFile(path);
    checkState(matcher.find());

    // Combine path metadata with file metadata
    return receiptFile()
        .projectId(matcher.group(1))
        .type(matcher.group(2))
        .study(matcher.group(3))
        .workflow(matcher.group(4))
        .analysisId(matcher.group(5))
        .timestamp(Long.parseLong(matcher.group(6)))
        .build();
  }

  @SneakyThrows
  private static ObjectNode readFile(Path path) {
    val file = path.toFile();
    val compressed = file.getName().endsWith(".gz");
    val fileStream = new FileInputStream(file);
    val inputStream = compressed ? new GZIPInputStream(fileStream) : fileStream;

    // Can't use jackson-dataformat-xml because of lack of repeating elements support, etc.
    val reader = new InputStreamReader(inputStream, UTF_8);
    val xml = CharStreams.toString(reader);
    val json = XML.toJSONObject(xml);

    return MAPPER.convertValue(json, ObjectNode.class);
  }

}
