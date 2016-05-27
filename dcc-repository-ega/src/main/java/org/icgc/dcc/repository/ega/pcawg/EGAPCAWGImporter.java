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
package org.icgc.dcc.repository.ega.pcawg;

import static org.icgc.dcc.common.core.util.Formats.formatCount;
import static org.icgc.dcc.repository.core.model.Repositories.getEGARepository;
import static org.icgc.dcc.repository.core.model.RepositorySource.EGA;

import java.io.File;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.util.GenericRepositorySourceFileImporter;
import org.icgc.dcc.repository.ega.pcawg.core.EGAFileProcessor;
import org.icgc.dcc.repository.ega.pcawg.model.EGASubmission;
import org.icgc.dcc.repository.ega.pcawg.reader.EGASubmissionReader;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * See https://github.com/ICGC-TCGA-PanCancer/ega-submission-tool for information on how the data was created for
 * import.
 * 
 * @see https://www.ebi.ac.uk/ega/dacs/EGAC00001000010
 */
@Slf4j
public class EGAPCAWGImporter extends GenericRepositorySourceFileImporter {

  /**
   * Constants.
   */
  private static final String GIT_REPO_URL = "https://github.com/ICGC-TCGA-PanCancer/pcawg-ega-submission.git";
  private static final File GIT_REPO_DIR = new File("/tmp/dcc-repository-ega");

  public EGAPCAWGImporter(RepositoryFileContext context) {
    super(EGA, context, log);
  }

  @Override
  protected Iterable<RepositoryFile> readFiles() {
    log.info("Reading submissions...");
    val submissions = readSubmissions();
    log.info("Finished reading {} submissions", formatCount(submissions));

    log.info("Processing files...");
    val files = processSubmissionFiles(submissions);
    log.info("Finished processing {} files", formatCount(files));

    return files;
  }

  private Iterable<EGASubmission> readSubmissions() {
    val reader = new EGASubmissionReader(GIT_REPO_URL, GIT_REPO_DIR);
    return reader.readSubmissions();
  }

  private Iterable<RepositoryFile> processSubmissionFiles(Iterable<EGASubmission> submission) {
    val processor = new EGAFileProcessor(context, getEGARepository());
    return processor.processSubmissions(submission);
  }

}
