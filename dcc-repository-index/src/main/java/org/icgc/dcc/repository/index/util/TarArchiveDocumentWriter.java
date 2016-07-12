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
package org.icgc.dcc.repository.index.util;

import static com.fasterxml.jackson.core.JsonGenerator.Feature.AUTO_CLOSE_TARGET;
import static org.icgc.dcc.common.core.util.Formats.formatBytes;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.CountingOutputStream;
import org.icgc.dcc.repository.index.core.RepositoryFileIndexes;
import org.icgc.dcc.repository.index.model.Document;
import org.icgc.dcc.repository.index.model.DocumentType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.ByteStreams;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * Knapsack compliant archive writer.
 * 
 * @see https://github.com/jprante/elasticsearch-knapsack
 */
@Slf4j
public class TarArchiveDocumentWriter implements Closeable {

  /**
   * Constants.
   */
  private static final ObjectMapper MAPPER = new ObjectMapper().configure(AUTO_CLOSE_TARGET, false);
  public static final String SETTINGS_FILE_NAME = "_settings";
  public static final String MAPPING_FILE_NAME = "_mapping";

  /**
   * Meta data.
   */
  private final String indexName;

  /**
   * State.
   */
  private final TarArchiveOutputStream archive;

  public TarArchiveDocumentWriter(@NonNull String indexName, @NonNull OutputStream outputStream) throws IOException {
    // Create state
    this.indexName = indexName;
    this.archive = createArchive(outputStream);

    addMetaEntries();
  }

  public void write(Document document) throws IOException {
    val name = formatEntryName(document);

    try {
      addEntry(name, document.getSource());
    } catch (Exception e) {
      throw new RuntimeException("Failed to write document with id " + document.getId() + " of type "
          + document.getType(), e);
    }
  }

  @Override
  @SneakyThrows
  public void close() {
    log.info("Bytes written: {}", formatBytes(archive.getBytesWritten()));

    log.info("Closing archive...");
    archive.finish();
    archive.close();
    log.info("Finished!");
  }

  private void addMetaEntries() throws IOException {
    addEntry(SETTINGS_FILE_NAME, RepositoryFileIndexes.getSettings());
    for (val type : DocumentType.values()) {
      val mappingEntryName = formatEntryName(type.getId(), MAPPING_FILE_NAME);

      addEntry(mappingEntryName, RepositoryFileIndexes.getTypeMapping(type.getId()));
    }
  }

  private void addEntry(String name, ObjectNode source) throws IOException {
    val size = getSize(source);

    // knapsack 2.x versions needs an extra directory so we embedded them under the index
    val entry = new TarArchiveEntry(formatEntryName(indexName, name));
    entry.setSize(size);

    archive.putArchiveEntry(entry);
    MAPPER.writeValue(archive, source);
    archive.closeArchiveEntry();
  }

  private static String formatEntryName(Document document) {
    return formatEntryName(document.getType().getId(), document.getId());
  }

  private static String formatEntryName(String parent, String child) {
    return String.format("%s/%s", parent, child);
  }

  private static TarArchiveOutputStream createArchive(OutputStream outputStream) throws IOException {
    return new TarArchiveOutputStream(outputStream);
  }

  @SneakyThrows
  private static long getSize(ObjectNode source) {
    val counter = new CountingOutputStream(ByteStreams.nullOutputStream());
    MAPPER.writeValue(counter, source);

    return counter.getBytesWritten();
  }

}
