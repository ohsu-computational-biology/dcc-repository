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
package org.icgc.dcc.repository.ega;

import static org.icgc.dcc.common.core.util.Formats.formatCount;
import static org.icgc.dcc.repository.core.model.RepositoryServers.getEGAServer;
import static org.icgc.dcc.repository.core.model.RepositorySource.EGA;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.util.GenericRepositorySourceFileImporter;
import org.icgc.dcc.repository.ega.core.EGASubmissionFileProcessor;
import org.icgc.dcc.repository.ega.model.EGASubmissionFile;
import org.icgc.dcc.repository.ega.submission.EGASubmissionReader;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * See https://github.com/ICGC-TCGA-PanCancer/ega-submission-tool
 */
@Slf4j
public class EGAImporter extends GenericRepositorySourceFileImporter {

  /**
   * Constants.
   */
  private static final String GIT_REPO_URL = "https://github.com/ICGC-TCGA-PanCancer/pcawg-ega-submission.git";

  public EGAImporter(RepositoryFileContext context) {
    super(EGA, context, log);
  }

  @Override
  protected Iterable<RepositoryFile> readFiles() {
    log.info("Reading submission files...");
    val details = readSubmissionFiles();
    log.info("Finished reading {} submission files", formatCount(details));

    log.info("Processing files...");
    val files = processSubmissionFiles(details);
    log.info("Finished processing {} files", formatCount(files));

    return files;
  }

  private Iterable<EGASubmissionFile> readSubmissionFiles() {
    val reader = new EGASubmissionReader(GIT_REPO_URL);
    return reader.readSubmissionFiles();
  }

  private Iterable<RepositoryFile> processSubmissionFiles(Iterable<EGASubmissionFile> submissionFiles) {
    val processor = new EGASubmissionFileProcessor(context, getEGAServer());
    return processor.processSubmissions(submissionFiles);
  }

}
