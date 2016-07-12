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
package org.icgc.dcc.repository.index.document;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import java.util.function.Consumer;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.icgc.dcc.repository.core.model.RepositoryCollection;
import org.icgc.dcc.repository.core.util.AbstractJongoComponent;
import org.icgc.dcc.repository.index.model.Document;
import org.icgc.dcc.repository.index.model.DocumentType;
import org.icgc.dcc.repository.index.util.TarArchiveDocumentWriter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.MongoClientURI;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

public abstract class DocumentProcessor extends AbstractJongoComponent {

  /**
   * Configuration.
   */
  @NonNull
  private final String indexName;
  @NonNull
  private final DocumentType type;

  /**
   * Dependencies.
   */
  @NonNull
  private final BulkProcessor bulkProcessor;
  @NonNull
  private final TarArchiveDocumentWriter archiveWriter;

  public DocumentProcessor(MongoClientURI mongoUri, String indexName, DocumentType type, BulkProcessor processor,
      TarArchiveDocumentWriter archiveWriter) {
    super(mongoUri);
    this.bulkProcessor = processor;
    this.archiveWriter = archiveWriter;
    this.indexName = indexName;
    this.type = type;
  }

  abstract public int process();

  protected int eachFile(Consumer<ObjectNode> consumer) {
    return eachDocument(RepositoryCollection.FILE, consumer);
  }

  protected Document createDocument(@NonNull String id) {
    return createDocument(id, DEFAULT.createObjectNode());
  }

  protected Document createDocument(@NonNull String id, @NonNull ObjectNode source) {
    return new Document(type, id, source);
  }

  @SneakyThrows
  protected void addDocument(Document document) {
    // Need to remove this as to not conflict with Elasticsearch
    val source = document.getSource();
    source.remove("_id");

    bulkProcessor.add(
        indexRequest(indexName)
            .type(type.getId())
            .id(document.getId())
            .source(serializeDocument(source)));

    archiveWriter.write(document);
  }

  protected static String getId(ObjectNode file) {
    return file.get("id").textValue();
  }

  protected static ArrayNode getDonors(ObjectNode file) {
    return file.withArray("donors");
  }

  protected static String getDonorId(JsonNode donor) {
    return donor.get("donor_id").textValue();
  }

  protected static String getSubmittedDonorId(JsonNode donor) {
    return donor.get("submitted_donor_id").textValue();
  }

  @SneakyThrows
  private static String serializeDocument(JsonNode document) {
    return DEFAULT.writeValueAsString(document);
  }

}
