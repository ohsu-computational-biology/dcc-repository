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
import org.icgc.dcc.repository.core.meta.Entity;
import org.icgc.dcc.repository.core.model.Repository;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileFormat;

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

  public PDCFileProcessor(RepositoryFileContext context, @NonNull Repository pdcRepository) {
    super(context);
    this.pdcRepository = pdcRepository;
  }

  public Iterable<RepositoryFile> processFiles(List<S3ObjectSummary> objectSummaries) {
    return objectSummaries.stream()
        .filter(this::isIncluded)
        .map(file -> createFile(file, objectSummaries))
        .collect(toList());
  }

  private RepositoryFile createFile(S3ObjectSummary objectSummary, List<S3ObjectSummary> objectSummaries) {
    val objectId = resolveObjectId(objectSummary);
    val entity = findEntity(objectId).get(); // Always present by context;

    val indexEntity = findIndexEntity(entity);
    val xmlEntity = findXmlEntity(entity);

    val objectFile = new RepositoryFile()
        .setId(context.ensureFileId(objectId))
        .setObjectId(objectId);

    val fileCopy = objectFile.addFileCopy()
        .setFileName(entity.getFileName())
        .setFileSize(objectSummary.getSize())
        .setFileFormat(resolveFileFormat(entity.getFileName()))
        .setLastModified(objectSummary.getLastModified().getTime() / 1000L) // Seconds
        .setRepoFileId(objectId)
        .setRepoType(pdcRepository.getType().getId())
        .setRepoOrg(pdcRepository.getSource().getId())
        .setRepoName(pdcRepository.getName())
        .setRepoCode(pdcRepository.getCode())
        .setRepoCountry(pdcRepository.getCountry())
        .setRepoBaseUrl(pdcRepository.getBaseUrl())
        .setRepoDataPath(objectSummary.getBucketName() + pdcRepository.getType().getDataPath() + objectId);

    if (xmlEntity.isPresent()) {
      val xmlSummary = resolveObjectSummary(objectSummaries, xmlEntity.get());
      if (xmlSummary.isPresent()) {
        val metadataPath =
            xmlSummary.get().getBucketName() + pdcRepository.getType().getMetadataPath() + xmlSummary.get().getKey();
        fileCopy
            .setRepoMetadataPath(metadataPath);
      }
    }

    if (indexEntity.isPresent()) {
      val indexSummary = resolveObjectSummary(objectSummaries, indexEntity.get());
      if (indexSummary.isPresent()) {
        fileCopy.getIndexFile()
            .setObjectId(indexEntity.get().getId())
            .setFileName(indexEntity.get().getFileName())
            .setFileSize(indexSummary.get().getSize())
            .setFileFormat(resolveFileFormat(indexEntity.get().getFileName()));
      }
    }

    return objectFile;
  }

  private Optional<S3ObjectSummary> resolveObjectSummary(List<S3ObjectSummary> objectSummaries, Entity entity) {
    return objectSummaries.stream().filter(s -> s.getKey().equals(entity.getId())).findFirst();
  }

  private boolean isIncluded(S3ObjectSummary objectSummary) {
    val objectId = resolveObjectId(objectSummary);
    val entity = findEntity(objectId);
    if (!entity.isPresent()) {
      log.warn("Could not find entity for object id {}", objectId);
      return false;
    }

    val fileName = entity.get().getFileName();
    if (!isBamFile(fileName) && !isVcfFile(fileName)) {
      return false;
    }

    return true;
  }

  private static String resolveObjectId(S3ObjectSummary objectSummary) {
    return objectSummary.getKey();
  }

  private static String resolveFileFormat(String fileName) {
    if (fileName.endsWith(".bam")) {
      return FileFormat.BAM;
    }
    if (fileName.endsWith(".bai")) {
      return FileFormat.BAI;
    }
    if (fileName.endsWith(".tbi")) {
      return FileFormat.TBI;
    }
    if (fileName.endsWith(".idx")) {
      return FileFormat.IDX;
    }
    if (fileName.endsWith(".vcf.gz")) {
      return FileFormat.VCF;
    }

    return null;
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

}
