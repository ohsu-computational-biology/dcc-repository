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
package org.icgc.dcc.repository.pdc.core;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Optional;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileProcessor;
import org.icgc.dcc.repository.core.model.Repository;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileCopy;
import org.icgc.dcc.repository.pdc.util.PCAWGFileResolver;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PDCFileProcessor extends RepositoryFileProcessor {

  /**
   * Metadata.
   */
  @NonNull
  private final Repository pdcRepository;

  /**
   * Dependencies.
   */
  private final PCAWGFileResolver resolver = new PCAWGFileResolver();

  public PDCFileProcessor(RepositoryFileContext context, @NonNull Repository pdcRepository) {
    super(context);
    this.pdcRepository = pdcRepository;
    log.warn("No XML files will be indexed!");
  }

  public Iterable<RepositoryFile> processFiles(List<S3ObjectSummary> objectSummaries) {
    return objectSummaries.stream()
        .filter(this::isIncluded)
        .map(file -> createFile(file, objectSummaries))
        .collect(toList());
  }

  private FileCopy resolvePCAWGFileCopy(String objectId) {
    return resolvePCAWGFile(objectId).getFileCopies().get(0);
  }

  private Optional<S3ObjectSummary> resolveObjectSummary(List<S3ObjectSummary> objectSummaries, String objectId) {
    return objectSummaries.stream().filter(s -> s.getKey().equals(objectId)).findFirst();
  }

  private boolean isIncluded(S3ObjectSummary objectSummary) {
    val objectId = resolveObjectId(objectSummary);
    val pcawgFile = resolvePCAWGFile(objectId);
    if (pcawgFile == null) {
      return false;
    }

    val fileName = pcawgFile.getFileCopies().get(0).getFileName();
    if (!isBamFile(fileName) && !isVcfFile(fileName)) {
      return false;
    }

    return true;
  }

  private RepositoryFile createFile(S3ObjectSummary objectSummary, List<S3ObjectSummary> objectSummaries) {
    val objectId = resolveObjectId(objectSummary);
    val pcawgFileCopy = resolvePCAWGFileCopy(objectId);

    val objectFile = new RepositoryFile()
        .setId(context.ensureFileId(objectId))
        .setObjectId(objectId);

    val fileCopy = objectFile.addFileCopy()
        .setFileName(pcawgFileCopy.getFileName())
        .setFileSize(objectSummary.getSize())
        .setFileFormat(pcawgFileCopy.getFileFormat())
        .setFileMd5sum(pcawgFileCopy.getFileMd5sum())
        .setLastModified(objectSummary.getLastModified().getTime() / 1000L) // Seconds
        .setRepoFileId(objectId)
        .setRepoDataBundleId(pcawgFileCopy.getRepoDataBundleId())
        .setRepoType(pdcRepository.getType().getId())
        .setRepoOrg(pdcRepository.getSource().getId())
        .setRepoName(pdcRepository.getName())
        .setRepoCode(pdcRepository.getCode())
        .setRepoCountry(pdcRepository.getCountry())
        .setRepoBaseUrl(pdcRepository.getBaseUrl())
        .setRepoDataPath(objectSummary.getBucketName() + pdcRepository.getType().getDataPath() + objectId);

    //
    // TODO: Add xml files when available.
    //

    if (pcawgFileCopy.getIndexFile() != null) {
      val pcawgIndexFile = pcawgFileCopy.getIndexFile();
      val indexSummary = resolveObjectSummary(objectSummaries, pcawgIndexFile.getObjectId());
      if (indexSummary.isPresent()) {
        fileCopy.getIndexFile()
            .setId(context.ensureFileId(pcawgIndexFile.getObjectId()))
            .setObjectId(pcawgIndexFile.getObjectId())
            .setFileName(pcawgIndexFile.getFileName())
            .setFileSize(indexSummary.get().getSize())
            .setFileMd5sum(pcawgIndexFile.getFileMd5sum())
            .setFileFormat(pcawgIndexFile.getFileFormat());
      }
    }

    return objectFile;
  }

  private static String resolveObjectId(S3ObjectSummary objectSummary) {
    return objectSummary.getKey();
  }

  private static boolean isBamFile(String fileName) {
    return hasFileExtension(fileName, ".bam");
  }

  private static boolean isVcfFile(String fileName) {
    return hasFileExtension(fileName, ".vcf.gz");
  }

  private static boolean hasFileExtension(String fileName, String fileType) {
    return fileName.toLowerCase().endsWith(fileType.toLowerCase());
  }

  private RepositoryFile resolvePCAWGFile(String objectId) {
    return resolver.resolve(objectId);
  }

}
