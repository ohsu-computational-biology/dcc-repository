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
package org.icgc.dcc.repository.client.index.document;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.icgc.dcc.common.core.util.Jackson.DEFAULT;

import java.util.function.Consumer;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.icgc.dcc.repository.client.index.model.DocumentType;
import org.icgc.dcc.repository.core.model.RepositoryFileCollection;
import org.icgc.dcc.repository.core.util.AbstractJongoComponent;

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

  public DocumentProcessor(MongoClientURI mongoUri, String indexName, DocumentType type, BulkProcessor processor) {
    super(mongoUri);
    this.bulkProcessor = processor;
    this.indexName = indexName;
    this.type = type;
  }

  abstract public int process();

  protected int eachFile(Consumer<ObjectNode> consumer) {
    return eachDocument(RepositoryFileCollection.FILE, consumer);
  }

  protected void addDocument(String id, ObjectNode document) {
    // Need to remove this as to not conflict with Elasticsearch
    document.remove("_id");

    val source = serializeDocument(document);
    bulkProcessor.add(
        indexRequest(indexName)
            .type(type.getId())
            .id(id)
            .source(source));
  }

  protected static ObjectNode createDocument() {
    return DEFAULT.createObjectNode();
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
