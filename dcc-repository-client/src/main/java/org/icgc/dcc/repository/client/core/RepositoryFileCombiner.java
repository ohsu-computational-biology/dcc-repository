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

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.repository.core.model.RepositorySource.PCAWG;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileCopy;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepositoryFileCombiner {

  public Iterable<RepositoryFile> combineFiles(Iterable<Set<RepositoryFile>> files) {
    log.info("Lazily combining files...");
    return new Iterable<RepositoryFile>() {

      @Override
      public Iterator<RepositoryFile> iterator() {

        Iterator<Set<RepositoryFile>> delegate = files.iterator();

        return new Iterator<RepositoryFile>() {

          @Override
          public RepositoryFile next() {
            return combineFiles(delegate.next());
          }

          @Override
          public boolean hasNext() {
            return delegate.hasNext();
          }

        };
      }

    };
  }

  private RepositoryFile combineFiles(Set<RepositoryFile> files) {
    // Round up all file copies
    val fileCopies = resolveFileCopies(files);

    // Designate one file as the representative for the group
    val representative = calculateRepresentative(files);

    // Combine all file copies
    representative.setFileCopies(fileCopies);

    return representative;
  }

  private List<FileCopy> resolveFileCopies(Set<RepositoryFile> files) {
    return files.stream()
        .flatMap(file -> file.getFileCopies().stream())
        .collect(toImmutableList());
  }

  private RepositoryFile calculateRepresentative(Set<RepositoryFile> files) {
    // Prioritize PCAWG ahead of others since it carries the most information
    return files.stream().sorted((f1, f2) -> isPCAWGFile(f1) ? -1 : 0).findFirst().get();
  }

  private static boolean isPCAWGFile(RepositoryFile file) {
    return file.getFileCopies().stream().anyMatch(fileCopy -> isPCAWGFileCopy(fileCopy));
  }

  private static boolean isPCAWGFileCopy(FileCopy fileCopy) {
    return fileCopy.getRepoOrg().equals(PCAWG.getId());
  }

}
