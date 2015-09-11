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
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.repository.core.model.RepositoryServers.getAWSServer;

import java.io.File;
import java.util.Set;
import java.util.function.Predicate;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileProcessor;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileAccess;
import org.icgc.dcc.repository.core.model.RepositoryServers.RepositoryServer;

import com.amazonaws.services.s3.model.S3ObjectSummary;

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

  public Iterable<RepositoryFile> processObjects(@NonNull Iterable<S3ObjectSummary> objectSummaries,
      @NonNull Set<String> objectIds) {
    log.info("Processing object summaries...");
    return stream(objectSummaries)
        .filter(isComplete(objectIds))
        .map(objectSummary -> processObject(objectSummary))
        .collect(toImmutableList());
  }

  private RepositoryFile processObject(S3ObjectSummary objectSummary) {
    log.debug("Processing bucket entry: {}", format("%-50s %10d %s",
        objectSummary.getKey(), objectSummary.getSize(), objectSummary.getStorageClass()));

    return createFile(objectSummary);
  }

  private RepositoryFile createFile(S3ObjectSummary s3Object) {

    //
    // Prepare
    //

    val id = getObjectId(s3Object);

    //
    // Create
    //

    val file = new RepositoryFile()
        .setId(id)
        .setFileId(context.ensureFileId(id))
        .setAccess(FileAccess.CONTROLLED);

    file.addFileCopy()
        .setFileName(id)
        .setFileFormat(null) // TODO: Fix
        .setFileSize(s3Object.getSize())
        .setFileMd5sum(null) // TODO: Fix
        .setLastModified(s3Object.getLastModified().getTime())
        .setIndexFile(null) // TODO: Fix
        .setRepoType(server.getType().getId())
        .setRepoOrg(server.getSource().getId())
        .setRepoName(server.getName())
        .setRepoCode(server.getCode())
        .setRepoCountry(server.getCountry())
        .setRepoBaseUrl(server.getBaseUrl())
        .setRepoDataPath(server.getType().getDataPath())
        .setRepoMetadataPath(server.getType().getMetadataPath());

    return file;
  }

  private static Predicate<? super S3ObjectSummary> isComplete(Set<String> completeObjectIds) {
    return objectSummary -> completeObjectIds.contains(getObjectId(objectSummary));
  }

  private static String getObjectId(S3ObjectSummary objectSummary) {
    return new File(objectSummary.getKey()).getName();
  }

}
