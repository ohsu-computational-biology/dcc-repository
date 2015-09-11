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
package org.icgc.dcc.repository.aws;

import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.repository.core.model.RepositorySource.AWS;

import java.util.List;
import java.util.Set;

import org.icgc.dcc.repository.aws.core.AWSCompletedIdResolver;
import org.icgc.dcc.repository.aws.core.AWSFileProcessor;
import org.icgc.dcc.repository.aws.reader.AWSS3BucketReader;
import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.util.GenericRepositorySourceFileImporter;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AWSImporter extends GenericRepositorySourceFileImporter {

  public AWSImporter(@NonNull RepositoryFileContext context) {
    super(AWS, context);
  }

  @Override
  protected Iterable<RepositoryFile> readFiles() {
    log.info("Reading complete object ids...");
    val completeObjectIds = readCompleteObjectIds();
    log.info("Read {} complete object ids", formatCount(completeObjectIds));

    log.info("Reading object summaries...");
    val objectSummaries = readObjectSummaries();
    log.info("Read {} object summaries", formatCount(objectSummaries));

    log.info("Processing files...");
    val files = processFiles(objectSummaries, completeObjectIds);
    log.info("Processed {} files", formatCount(files));

    return files;
  }

  private Set<String> readCompleteObjectIds() {
    return new AWSCompletedIdResolver().resolveIds();
  }

  private Iterable<RepositoryFile> processFiles(List<S3ObjectSummary> objectSummaries, Set<String> completeObjectIds) {
    return new AWSFileProcessor(context).processObjects(objectSummaries, completeObjectIds);
  }

  private List<S3ObjectSummary> readObjectSummaries() {
    return new AWSS3BucketReader().readSummaries();
  }

}
