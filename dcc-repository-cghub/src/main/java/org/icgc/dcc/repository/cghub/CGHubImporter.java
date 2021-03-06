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
package org.icgc.dcc.repository.cghub;

import static org.icgc.dcc.repository.core.model.RepositorySource.CGHUB;

import org.icgc.dcc.repository.cghub.core.CGHubFileProcessor;
import org.icgc.dcc.repository.cghub.reader.CGHubAnalysisDetailReader;
import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.util.GenericRepositorySourceFileImporter;

import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * @see https://tcga-data.nci.nih.gov/datareports/codeTablesReport.htm
 */
@Slf4j
public class CGHubImporter extends GenericRepositorySourceFileImporter {

  public CGHubImporter(RepositoryFileContext context) {
    super(CGHUB, context, log);
  }

  @Override
  protected Iterable<RepositoryFile> readFiles() {
    log.info("Reading details...");
    val details = readDetails();
    log.info("Finished reading details");

    log.info("Processing details...");
    val files = processDetails(details);
    log.info("Finished processing details");

    return files;
  }

  private Iterable<ObjectNode> readDetails() {
    return new CGHubAnalysisDetailReader().readDetails();
  }

  private Iterable<RepositoryFile> processDetails(Iterable<ObjectNode> details) {
    val processor = new CGHubFileProcessor(context);
    return processor.processDetails(details);
  }

}
