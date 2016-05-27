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
package org.icgc.dcc.repository.pdc;

import static org.icgc.dcc.common.core.util.Formats.formatCount;
import static org.icgc.dcc.repository.core.model.Repositories.getPDCRepository;
import static org.icgc.dcc.repository.core.model.RepositorySource.PDC;

import java.util.List;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.util.GenericRepositorySourceFileImporter;
import org.icgc.dcc.repository.pdc.core.PDCFileProcessor;
import org.icgc.dcc.repository.pdc.s3.AWSClientFactory;
import org.icgc.dcc.repository.pdc.s3.PDCBucketReader;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PDCImporter extends GenericRepositorySourceFileImporter {

  public PDCImporter(@NonNull RepositoryFileContext context) {
    super(PDC, context, log);
  }

  @Override
  protected Iterable<RepositoryFile> readFiles() {
    log.info("Reading object summaries...");
    val objectSummaries = readObjectSummaries();
    log.info("Read {} object summaries", formatCount(objectSummaries));

    log.info("Processing files...");
    val files = processFiles(objectSummaries);
    log.info("Processed {} files", formatCount(files));

    return files;
  }

  private List<S3ObjectSummary> readObjectSummaries() {
    val s3 = AWSClientFactory.createS3Client();
    val bucketReader = new PDCBucketReader(s3);
    return bucketReader.readSummaries();
  }

  private Iterable<RepositoryFile> processFiles(List<S3ObjectSummary> objectSummaries) {
    val fileProcessor = new PDCFileProcessor(context, getPDCRepository());
    return fileProcessor.processFiles(objectSummaries);
  }

}
