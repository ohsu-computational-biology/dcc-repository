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
package org.icgc.dcc.repository.client.core;

import static com.google.common.collect.Iterables.size;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.repository.core.model.RepositoryServers.RepositoryCodes.AWS_VIRGINIA;
import static org.icgc.dcc.repository.core.model.RepositoryServers.RepositoryCodes.COLLABORATORY;

import java.util.Set;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileCopy;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RepositoryFileFilter {

  /**
   * Dependencies.
   */
  @NonNull
  private final RepositoryFileContext context;

  public Iterable<RepositoryFile> filterFiles(Iterable<RepositoryFile> files) {
    log.info("Filtering files...");
    val filteredFiles = stream(files).filter(this::isIncluded).collect(toImmutableList());
    log.info("Filtered {} files", formatCount(size(files) - filteredFiles.size()));

    return filteredFiles;
  }

  private boolean isIncluded(RepositoryFile file) {
    val repoCodes = getRepoCodes(file);

    // PCAWG published
    val pcawg = containsPCAWG(repoCodes);
    if (pcawg) {
      return true;
    }

    // Not released via PCAWG yet ignore
    val aws = repoCodes.contains(AWS_VIRGINIA);
    val collab = repoCodes.contains(COLLABORATORY);
    if (aws || collab) {
      return false;
    }

    // All others are ok
    return true;
  }

  private boolean containsPCAWG(Set<String> repoCodes) {
    for (val repoCode : repoCodes) {
      // TODO: Use RepositorySource instead
      if (repoCode.toLowerCase().startsWith("pcawg")) {
        return true;
      }
    }

    return false;
  }

  private static Set<String> getRepoCodes(RepositoryFile file) {
    return file.getFileCopies().stream().map(FileCopy::getRepoCode).collect(toImmutableSet());
  }

}
