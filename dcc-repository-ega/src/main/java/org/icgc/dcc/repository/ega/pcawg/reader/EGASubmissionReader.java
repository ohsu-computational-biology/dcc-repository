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
package org.icgc.dcc.repository.ega.pcawg.reader;

import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.collect.Ordering.natural;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.repository.ega.pcawg.model.EGASubmission.submission;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import org.eclipse.jgit.api.errors.GitAPIException;
import org.icgc.dcc.repository.core.util.TransferMetadataRepository;
import org.icgc.dcc.repository.ega.pcawg.model.EGAAnalysisFile;
import org.icgc.dcc.repository.ega.pcawg.model.EGAGnosFile;
import org.icgc.dcc.repository.ega.pcawg.model.EGAReceiptFile;
import org.icgc.dcc.repository.ega.pcawg.model.EGASampleFile;
import org.icgc.dcc.repository.ega.pcawg.model.EGAStudyFile;
import org.icgc.dcc.repository.ega.pcawg.model.EGASubmission;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.TreeMultimap;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

@RequiredArgsConstructor
public class EGASubmissionReader {

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
    updateMetadata();

    // Read and assemble
    return createSubmissions();
  }

  private void updateMetadata() throws GitAPIException, IOException {
    val repository = new TransferMetadataRepository(repoUrl, repoDir);
    repository.update();
  }

  private List<EGASubmission> createSubmissions() {
    // Read sources
    val studyFiles = readStudyFiles();
    val sampleFiles = readSampleFiles();
    val gnosFiles = readGnosFiles();
    val analysisFiles = readAnalysisFiles();
    val receiptFiles = readReceiptFiles();

    // Index sources for lookup in combine step
    val studyIndex = uniqueIndex(studyFiles, EGAStudyFile::getStudy);
    val gnosIndex = uniqueIndex(gnosFiles, EGAGnosFile::getAnalysisId);

    val sampleIndex = HashMultimap.<String, EGASampleFile> create();
    sampleFiles.forEach(f -> sampleIndex.put(f.getProjectId(), f));

    val receiptIndex = TreeMultimap.<String, EGAReceiptFile> create(natural(), timeDecending());
    receiptFiles.forEach(f -> receiptIndex.put(f.getAnalysisId(), f));

    // Combine both files into a merged record
    return analysisFiles.stream()
        .map(f -> submission()
            .studyFile(studyIndex.get(f.getStudy()))
            .sampleFiles(sampleIndex.get(f.getProjectId()))
            .gnosFile(gnosIndex.get(f.getAnalysisId()))
            .receiptFile(getLatestReceipt(receiptIndex, f.getAnalysisId()))
            .analysisFile(f)
            .build())
        .collect(toImmutableList());
  }

  private List<EGAStudyFile> readStudyFiles() {
    return new EGAStudyFileReader(repoDir).readFiles();
  }

  private List<EGASampleFile> readSampleFiles() {
    return new EGASampleFileReader(repoDir).readFiles();
  }

  private List<EGAGnosFile> readGnosFiles() {
    return new EGAGnosFileReader(repoDir).readFiles();
  }

  private List<EGAAnalysisFile> readAnalysisFiles() {
    return new EGAAnalysisFileReader(repoDir).readFiles();
  }

  private List<EGAReceiptFile> readReceiptFiles() {
    return new EGAReceiptFileReader(repoDir).readFiles();
  }

  private static EGAReceiptFile getLatestReceipt(TreeMultimap<String, EGAReceiptFile> receiptIndex, String analysisId) {
    return getFirst(receiptIndex.get(analysisId), null);
  }

  private static Comparator<? super EGAReceiptFile> timeDecending() {
    return natural().onResultOf((EGAReceiptFile f) -> f.getTimestamp()).reversed();
  }

}
