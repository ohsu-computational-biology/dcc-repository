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
package org.icgc.dcc.repository.exacloud.core;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.exacloud.model.ExacloudArchiveClinicalFile;
import org.icgc.dcc.repository.exacloud.model.ExacloudArchiveManifestEntry;
import org.icgc.dcc.repository.exacloud.model.ExacloudArchivePageEntry;
import org.icgc.dcc.repository.exacloud.reader.ExacloudArchiveManifestReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.icgc.dcc.common.core.util.Splitters.TAB;
import static org.icgc.dcc.common.core.util.URLs.getUrl;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

@Slf4j
public class ExacloudArchiveClinicalFileProcessor {

  /**
   * Constants.
   */
  private static final Pattern CLINICAL_FILENAME_PATTERN = Pattern.compile(".*_clinical.([^.]+).xml");

  public List<RepositoryFile> process(@NonNull String archiveUrl, String projectCode) {
    log.info("Processing archive url '{}'...", archiveUrl);
    val archiveFolderUrl = resolveArchiveFolderUrl(archiveUrl);
//    val md5s = resolveArchiveFileMD5Sums(archiveFolderUrl);

    val clinicalFiles = ImmutableList.<ExacloudArchiveClinicalFile> builder();

    val url = getUrl(archiveUrl);


    try {
      val lines = Resources.readLines(url, UTF_8);
      val files = lines.stream()
              .skip(1)
              .map(TAB::splitToList)
              .map(values -> createFile()
                      .setId(values.get(3))
                      .setDonors(
                              Arrays.asList(new RepositoryFile.Donor()
                                  .setDonorId(values.get(1))
                                  .setSampleId(Arrays.asList(values.get(2)))
                                  .setProjectCode(projectCode)
                                  .setSubmittedDonorId(values.get(1))
                                  .setSubmittedSampleId(Arrays.asList(values.get(2)))
                              )
                      )
                      .setFileCopies(Arrays.asList(new RepositoryFile.FileCopy()
                                    .setFileName(values.get(5))
                                    .setRepoFileId(values.get(3))
                                    .setFileFormat(values.get(4))
                              )
                      )
                  )
              .collect(toList());
      return files;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;

//    0 = {String@2032} "research-only"
//    1 = {String@2033} "AML-1011"
//    2 = {String@2034} "AML-13-00098"
//    3 = {String@2035} "5e939aa5-26bc-4be5-b017-e63e878cb4c2"
//    4 = {String@2036} "application/octet-stream"
//    5 = {String@2037} "/mnt/lustre1/BeatAML/rnaseq/processed/BeatAML1/alignments/subjunc_alignments/FlowCell1/Sample_13-00098.bai"
//    6 = {String@2038} ""
//    7 = {String@2039} ""
//
//    List<List<String>> x  =  lines.stream()
//      .skip(1)
//      .map(TAB::splitToList);
//      .map(values -> publishedFile()
//        .datasetId(values.get(0))
//        .analysisId(values.get(1))
//        .fileId(values.get(2))
//        .fileName(values.get(3))
//        .build())
//      .collect(toList());



//    for (val entry : ExacloudArchivePageReader.readEntries(archiveFolderUrl)) {
//
//      val clinical = matchClinicalFileName(entry.getFileName());
//      if (!clinical.isPresent()) {
//        continue;
//      }
//
//      val donorId = clinical.get();
//      val url = archiveFolderUrl + "/" + entry.getFileName();
////      val md5 = md5s.get(entry.getFileName());
//      val md5 = "";
//      val clinicalFile =
//          new ExacloudArchiveClinicalFile(
//              donorId, entry.getFileName(), resolveLastModified(entry),
//              entry.getFileSize(), md5, url);
//
//      clinicalFiles.add(clinicalFile);
//    }

//    return clinicalFiles.build();
  }

  private RepositoryFile createFile() {
    return new RepositoryFile();
  }
  private static long resolveLastModified(ExacloudArchivePageEntry entry) {
    return entry.getLastModified().getEpochSecond();
  }

  private static Map<String, String> resolveArchiveFileMD5Sums(String archiveFolderUrl) {
    val entries = ExacloudArchiveManifestReader.readEntries(archiveFolderUrl);
    val md5ByFileName = toMap(ExacloudArchiveManifestEntry::getFileName, ExacloudArchiveManifestEntry::getMd5);
    return stream(entries).collect(md5ByFileName);
  }

  private static String resolveArchiveFolderUrl(String archiveUrl) {
    return archiveUrl.replaceFirst(".tar.gz$", "");
  }

  private static Optional<String> matchClinicalFileName(String fileName) {
    val matcher = CLINICAL_FILENAME_PATTERN.matcher(fileName);
    return Optional.ofNullable(matcher.matches() ? matcher.group(1) : null);
  }

}
