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
package org.icgc.dcc.repository.pdc.util;

import static com.google.common.collect.Iterables.getFirst;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.URLs.getUrl;
import static org.icgc.dcc.repository.core.model.RepositorySource.PCAWG;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.PCAWG_ARCHIVE_BASE_URL;

import java.io.IOException;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileContextBuilder;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.pcawg.core.PCAWGFileProcessor;
import org.icgc.dcc.repository.pcawg.reader.PCAWGDonorArchiveReader;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PCAWGFileResolver {

  /**
   * State.
   */
  @Getter(lazy = true, value = PRIVATE)
  private final Multimap<String, RepositoryFile> files = resolveFiles();

  public RepositoryFile resolve(@NonNull String objectId) {
    val files = getFiles().get(objectId);
    if (files.size() > 1) {
      log.warn("More than one file found for object id {}: {}", objectId, files);
    }

    return getFirst(files, null);
  }

  @SneakyThrows
  private static Multimap<String, RepositoryFile> resolveFiles() {
    val watch = Stopwatch.createStarted();
    val context = createFileContext();

    log.info("Resolving PCAWG files...");
    val donors = readDonors();
    val files = readFiles(context, donors);
    log.info("Resolved PCAWG files in {}", watch);

    return Multimaps.index(files, RepositoryFile::getObjectId);
  }

  public static Iterable<ObjectNode> readDonors() throws IOException {
    // This URL is required as it includes a point in time where all the PDC files exist in GNOS repos to acquire all
    // metadata
    val url = getUrl(PCAWG_ARCHIVE_BASE_URL + "/data_releases/latest/release_may2016.v1.1.jsonl");
    return new PCAWGDonorArchiveReader(url).readDonors();
  }

  public static Iterable<RepositoryFile> readFiles(RepositoryFileContext context, Iterable<ObjectNode> donors) {
    return new PCAWGFileProcessor(context) {

      @Override
      protected void translateUUIDs(Iterable<RepositoryFile> donorFiles) {
        // No-op since this takes a while and is not needed here.
      };

    }.processDonors(donors);
  }

  private static RepositoryFileContext createFileContext() {
    return RepositoryFileContextBuilder
        .builder()
        .geneMongoUri(null)
        .realIds(false)
        .sources(ImmutableSet.of(PCAWG))
        .indexAlias("")
        .pcawgIdResolver(() -> ImmutableSet.of())
        .dccIdResolver(() -> ImmutableSet.of())
        .build();
  }

}
