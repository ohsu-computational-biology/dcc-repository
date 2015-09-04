/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.repository.core;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.model.ReleaseCollection.FILE_COLLECTION;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;

import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositorySource;
import org.icgc.dcc.repository.core.util.AbstractJongoWriter;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.jongo.MongoCollection;

import com.mongodb.MongoClientURI;

@Slf4j
public class RepositorySourceFileWriter extends AbstractJongoWriter<Iterable<RepositoryFile>> {

  /**
   * Constants.
   */
  public static final String FILE_REPOSITORY_ORG_FIELD_NAME = "repository.repo_org";

  /**
   * Metadata.
   */
  @NonNull
  protected final RepositorySource source;

  /**
   * Dependencies.
   */
  @NonNull
  protected final MongoCollection fileCollection;

  public RepositorySourceFileWriter(@NonNull MongoClientURI mongoUri, @NonNull RepositorySource source) {
    super(mongoUri);
    this.fileCollection = getCollection(FILE_COLLECTION);
    this.source = source;
  }

  @Override
  public void write(@NonNull Iterable<RepositoryFile> files) {
    log.info("Clearing file documents...");
    clearFiles();

    log.info("Writing file documents...");
    for (val file : files) {
      saveFile(file);
    }
  }

  public void clearFiles() {
    log.info("Clearing '{}' documents in collection '{}'", source.getId(), fileCollection.getName());
    val result = fileCollection.remove("{ " + FILE_REPOSITORY_ORG_FIELD_NAME + ": # }", source.getId());
    checkState(result.getLastError().ok(), "Error clearing mongo: %s", result);

    log.info("Finished clearing {} '{}' documents in collection '{}'",
        formatCount(result.getN()), source.getId(), fileCollection.getName());
  }

  protected void saveFile(RepositoryFile file) {
    fileCollection.save(file);
  }

}