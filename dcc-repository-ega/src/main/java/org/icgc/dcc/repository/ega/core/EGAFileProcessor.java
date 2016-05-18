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
package org.icgc.dcc.repository.ega.core;

import static com.google.common.collect.Iterables.getFirst;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.Collections.singletonList;
import static org.icgc.dcc.repository.ega.util.EGAMappings.getMappingDataSetId;
import static org.icgc.dcc.repository.ega.util.EGAMappings.getMappingFileId;
import static org.icgc.dcc.repository.ega.util.EGAMappings.getMappingFileName;
import static org.icgc.dcc.repository.ega.util.EGAMappings.getMappingFileSize;
import static org.icgc.dcc.repository.ega.util.EGARuns.getRunChecksum;
import static org.icgc.dcc.repository.ega.util.EGARuns.getRunDate;
import static org.icgc.dcc.repository.ega.util.EGARuns.getRunFile;
import static org.icgc.dcc.repository.ega.util.EGARuns.getRunFileName;
import static org.icgc.dcc.repository.ega.util.EGARuns.getRunFileType;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Set;
import java.util.stream.Stream;

import org.icgc.dcc.common.core.util.Formats;
import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileProcessor;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileCopy;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileFormat;
import org.icgc.dcc.repository.core.model.RepositoryServers.RepositoryServer;
import org.icgc.dcc.repository.ega.model.EGAMetadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EGAFileProcessor extends RepositoryFileProcessor {

  public static Set<String> formats = Sets.newTreeSet();
  public static int count = 0;

  /**
   * Metadata.
   */
  @NonNull
  private final RepositoryServer egaServer;

  public EGAFileProcessor(RepositoryFileContext context, @NonNull RepositoryServer egaServer) {
    super(context);
    this.egaServer = egaServer;
  }

  public Stream<RepositoryFile> process(Stream<EGAMetadata> metadata) {
    return metadata.flatMap(this::createFiles);
  }

  private Stream<RepositoryFile> createFiles(EGAMetadata metadata) {
    return metadata.getFiles().stream().map(file -> createFile(metadata, file));
  }

  private RepositoryFile createFile(EGAMetadata metadata, ObjectNode file) {
    val egaFile = new RepositoryFile();

    egaFile.getDataBundle()
        .setDataBundleId(metadata.getDatasetId());

    val fileCopy = egaFile.addFileCopy()
        .setRepoDataBundleId(metadata.getDatasetId())
        .setRepoFileId(getMappingFileId(file))
        .setRepoDataSetId(getMappingDataSetId(file))
        .setFileSize(getMappingFileSize(file))
        .setFileName(null) // Set from run
        .setRepoType(egaServer.getType().getId())
        .setRepoOrg(egaServer.getSource().getId())
        .setRepoName(egaServer.getName())
        .setRepoCode(egaServer.getCode())
        .setRepoCountry(egaServer.getCountry())
        .setRepoBaseUrl(egaServer.getBaseUrl())
        .setRepoMetadataPath(egaServer.getType().getMetadataPath())
        .setRepoDataPath(egaServer.getType().getDataPath());

    updateFileCopy(fileCopy, getMappingFileName(file), metadata);

    egaFile.addDonor()
        .setProjectCode(getFirst(metadata.getProjectCodes(), null));

    val fileFormat = egaFile.getFileCopies().get(0).getFileFormat();
    formats.add(fileFormat == null ? "<empty>" : fileFormat);

    log.info("[{}] formats: {}", Formats.formatCount(++count), formats);

    return egaFile;
  }

  private void updateFileCopy(FileCopy fileCopy, String fileName, EGAMetadata metadata) {

    for (val root : metadata.getMetadata().getRuns().values()) {
      val files = resolveRunFiles(root);
      for (val file : files) {
        val match = resolveFileFormat(fileName).contains(getRunFileName(file));
        if (match) {
          fileCopy
              .setFileName(getRunFileName(file))
              .setFileFormat(resolveFileFormat(getRunFileType(file)))
              .setFileMd5sum(getRunChecksum(file))
              .setLastModified(resolveLastModified(getRunDate(root)));
        }
      }
    }
  }

  private static String resolveFileFormat(String fileType) {
    if ("bam".equals(fileType)) {
      return FileFormat.BAM;
    }
    if ("fastq".equals(fileType)) {
      return FileFormat.FASTQ;
    }

    return fileType;
  }

  private static Long resolveLastModified(String runDate) {
    if (runDate == null) {
      return null;
    }

    val dateTime = LocalDateTime.parse(resolveFileFormat(runDate), ISO_DATE_TIME);
    val egaTimeZone = ZoneId.of("Europe/London");
    return dateTime.atZone(egaTimeZone).toInstant().toEpochMilli();
  }

  private static Iterable<JsonNode> resolveRunFiles(JsonNode root) {
    val values = getRunFile(root);
    return values.isArray() ? values : singletonList(values);
  }

}
