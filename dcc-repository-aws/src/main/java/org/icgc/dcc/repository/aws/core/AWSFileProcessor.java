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
package org.icgc.dcc.repository.aws.core;

import static java.lang.String.format;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.repository.aws.util.AWSS3TransferJobs.getFileMd5sum;
import static org.icgc.dcc.repository.aws.util.AWSS3TransferJobs.getFileName;
import static org.icgc.dcc.repository.aws.util.AWSS3TransferJobs.getFileSize;
import static org.icgc.dcc.repository.aws.util.AWSS3TransferJobs.getFiles;
import static org.icgc.dcc.repository.aws.util.AWSS3TransferJobs.getGnosId;
import static org.icgc.dcc.repository.aws.util.AWSS3TransferJobs.getObjectId;
import static org.icgc.dcc.repository.core.model.RepositoryServers.getAWSServer;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileProcessor;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileAccess;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileFormat;
import org.icgc.dcc.repository.core.model.RepositoryServers.RepositoryServer;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AWSFileProcessor extends RepositoryFileProcessor {

  /**
   * Metadata.
   */
  private final RepositoryServer server = getAWSServer();

  public AWSFileProcessor(RepositoryFileContext context) {
    super(context);
  }

  public Iterable<RepositoryFile> processCompletedJobs(@NonNull List<ObjectNode> completedJobs,
      @NonNull Iterable<S3ObjectSummary> objectSummaries) {
    log.info("Indexing object summaries...");
    val objectSummaryIndex = indexObjectSummaries(objectSummaries);
    log.info("Finished indexing {} object summaries...", formatCount(objectSummaries));

    log.info("Creating object files...");
    val objectFiles = createObjectFiles(completedJobs, objectSummaryIndex);
    log.info("Finished creating {} object files", formatCount(objectFiles));

    return objectFiles;
  }

  private Iterable<RepositoryFile> createObjectFiles(List<ObjectNode> completedJobs,
      Map<String, S3ObjectSummary> objectSummaryIndex) {
    val objectFiles = ImmutableList.<RepositoryFile> builder();
    for (val completedJob : completedJobs) {

      for (val file : resolveIncludedFiles(completedJob)) {
        val objectId = getObjectId(file);
        val objectSummary = objectSummaryIndex.get(objectId);
        val objectFile = createObjectFile(completedJob, file, objectSummary);

        objectFiles.add(objectFile);
      }
    }

    return objectFiles.build();
  }

  private RepositoryFile createObjectFile(ObjectNode job, JsonNode file, S3ObjectSummary objectSummary) {
    log.debug("Processing bucket entry: {}", format("%-50s %10d %s",
        objectSummary.getKey(), objectSummary.getSize(), objectSummary.getStorageClass()));

    //
    // Prepare
    //

    val id = getObjectId(file);
    val gnosId = getGnosId(job);
    val fileName = getFileName(file);
    val xmlFile = resolveXmlFile(job, gnosId);
    val baiFile = resolveBaiFile(job, fileName);

    //
    // Create
    //

    val objectFile = new RepositoryFile()
        .setId(id)
        .setFileId(context.ensureFileId(id))
        .setAccess(FileAccess.CONTROLLED);

    val fileCopy = objectFile.addFileCopy()
        .setFileName(fileName)
        .setFileFormat(resolveFileFormat(file))
        .setFileSize(objectSummary.getSize())
        .setFileMd5sum(getFileMd5sum(file))
        .setLastModified(objectSummary.getLastModified().getTime() / 1000L) // Seconds
        .setRepoType(server.getType().getId())
        .setRepoOrg(server.getSource().getId())
        .setRepoName(server.getName())
        .setRepoCode(server.getCode())
        .setRepoCountry(server.getCountry())
        .setRepoBaseUrl(server.getBaseUrl())
        .setRepoDataPath(server.getType().getDataPath() + "/" + id);

    if (xmlFile.isPresent()) {
      val xmlId = getObjectId(xmlFile.get());
      fileCopy
          .setRepoMetadataPath(server.getType().getMetadataPath() + "/" + xmlId);
    }

    if (baiFile.isPresent()) {
      val baiFileName = getFileName(baiFile.get());
      val baiId = resolveId(gnosId, baiFileName);
      fileCopy.getIndexFile()
          .setId(baiId)
          .setFileId(context.ensureFileId(baiId))
          .setFileName(baiFileName)
          .setFileFormat(FileFormat.BAI)
          .setFileSize(getFileSize(baiFile.get()))
          .setFileMd5sum(getFileMd5sum(baiFile.get()));
    }

    return objectFile;
  }

  private static Iterable<JsonNode> resolveIncludedFiles(ObjectNode job) {
    return resolveFiles(job, file -> isBamFile(file) || isVcfFile(file)).collect(toImmutableList());
  }

  private static String resolveFileFormat(JsonNode file) {
    return isBamFile(file) ? FileFormat.BAM : FileFormat.VCF;
  }

  private static boolean isBamFile(JsonNode file) {
    return hasFileExtension(file, ".bam");
  }

  private static boolean isVcfFile(JsonNode file) {
    return hasFileExtension(file, ".vcf.gz");
  }

  private static boolean hasFileExtension(JsonNode file, String fileType) {
    return getFileName(file).toLowerCase().endsWith(fileType.toLowerCase());
  }

  private static Optional<JsonNode> resolveBaiFile(ObjectNode job, String fileName) {
    val baiFileName = fileName + ".bai";
    return resolveFiles(job, file -> baiFileName.equals(getFileName(file))).findFirst();
  }

  private static Optional<JsonNode> resolveXmlFile(ObjectNode job, String gnosId) {
    val xmlFileName = gnosId + ".xml";
    return resolveFiles(job, file -> xmlFileName.equals(getFileName(file))).findFirst();
  }

  private static Stream<JsonNode> resolveFiles(ObjectNode job, Predicate<? super JsonNode> filter) {
    return stream(getFiles(job)).filter(filter);
  }

  private static Map<String, S3ObjectSummary> indexObjectSummaries(Iterable<S3ObjectSummary> objectSummaries) {
    return Maps.uniqueIndex(objectSummaries, objectSummary -> getS3ObjectId(objectSummary));
  }

  private static String getS3ObjectId(S3ObjectSummary objectSummary) {
    return new File(objectSummary.getKey()).getName();
  }

}
