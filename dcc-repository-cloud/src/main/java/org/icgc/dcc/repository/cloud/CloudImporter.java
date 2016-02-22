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
package org.icgc.dcc.repository.cloud;

import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;

import java.util.List;

import org.icgc.dcc.repository.cloud.core.CloudFileProcessor;
import org.icgc.dcc.repository.cloud.s3.CloudS3BucketReader;
import org.icgc.dcc.repository.cloud.transfer.CloudTransferJobReader;
import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositorySource;
import org.icgc.dcc.repository.core.util.GenericRepositorySourceFileImporter;
import org.slf4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.NonNull;
import lombok.val;

public abstract class CloudImporter extends GenericRepositorySourceFileImporter {

  public CloudImporter(@NonNull RepositorySource source, @NonNull RepositoryFileContext context, @NonNull Logger log) {
    super(source, context, log);
  }

  @Override
  protected Iterable<RepositoryFile> readFiles() {
    log.info("Reading completed transfer jobs...");
    val completedJobs = readCompletedJobs();
    log.info("Read {} completed transfer jobs", formatCount(completedJobs));

    log.info("Reading object summaries...");
    val objectSummaries = readObjectSummaries();
    log.info("Read {} object summaries", formatCount(objectSummaries));

    log.info("Processing files...");
    val files = processFiles(completedJobs, objectSummaries);
    log.info("Processed {} files", formatCount(files));

    return files;
  }

  private List<ObjectNode> readCompletedJobs() {
    val jobReader = createJobReader();
    return jobReader.readJobs();
  }

  private List<S3ObjectSummary> readObjectSummaries() {
    val bucketReader = createBucketReader();
    return bucketReader.readSummaries();
  }

  private Iterable<RepositoryFile> processFiles(List<ObjectNode> completedJobs, List<S3ObjectSummary> objectSummaries) {
    val fileProcessor = createFileProcessor();
    return fileProcessor.processCompletedJobs(completedJobs, objectSummaries);
  }

  /**
   * Template methods.
   */

  abstract protected CloudTransferJobReader createJobReader();

  abstract protected CloudS3BucketReader createBucketReader();

  abstract protected CloudFileProcessor createFileProcessor();

}
