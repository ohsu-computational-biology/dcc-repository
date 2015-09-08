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
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

import java.util.Iterator;
import java.util.Set;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositorySource;
import org.icgc.dcc.repository.core.reader.RepositorySourceFileReader;

import com.google.common.collect.HashMultimap;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RepositoryFileCollector {

  /**
   * Dependencies.
   */
  @NonNull
  private final RepositoryFileContext context;

  public Iterable<Set<RepositoryFile>> collectFiles() {
    log.info("Collecting files...");
    val readers = createReaders();
    val files = readFiles(readers);

    return iterable(files);

  }

  private HashMultimap<String, RepositoryFile> readFiles(Iterable<RepositorySourceFileReader> readers) {
    val files = HashMultimap.<String, RepositoryFile> create();
    for (val reader : readers) {
      for (val file : reader.read()) {
        files.put(file.getId(), file);
      }
    }
    return files;
  }

  private Iterable<RepositorySourceFileReader> createReaders() {
    return stream(RepositorySource.values())
        .map(source -> new RepositorySourceFileReader(context.getMongoUri(), source))
        .collect(toImmutableList());
  }

  private Iterable<Set<RepositoryFile>> iterable(HashMultimap<String, RepositoryFile> files) {
    return new Iterable<Set<RepositoryFile>>() {

      @Override
      public Iterator<Set<RepositoryFile>> iterator() {

        Iterator<String> delegate = files.keySet().iterator();

        return new Iterator<Set<RepositoryFile>>() {

          @Override
          public boolean hasNext() {
            return delegate.hasNext();
          }

          @Override
          public Set<RepositoryFile> next() {
            return files.get(delegate.next());
          }

        };
      }
    };
  }

}
