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

import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.icgc.dcc.repository.core.model.RepositoryServers;
import org.icgc.dcc.repository.core.model.RepositoryServers.RepositoryServer;
import org.icgc.dcc.repository.index.model.DocumentType;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.MongoClientURI;

import lombok.val;

public class RepositoryDocumentProcessor extends DocumentProcessor {

  public RepositoryDocumentProcessor(MongoClientURI mongoUri, String indexName, BulkProcessor processor) {
    super(mongoUri, indexName, DocumentType.REPOSITORY, processor);
  }

  @Override
  public int process() {
    int count = 0;
    for (val server : RepositoryServers.getServers()) {
      val id = server.getCode();
      val document = createDocument(server);
      addDocument(id, document);
      count++;
    }

    return count;
  }

  private ObjectNode createDocument(RepositoryServer server) {
    return object()
        .with("id", server.getCode())
        .with("code", server.getCode())
        .with("type", server.getType().getId())
        .with("name", server.getName())
        .with("source", server.getSource().getId())
        .with("country", server.getCountry())
        .with("baseUrl", server.getBaseUrl())
        .with("dataPath", server.getType().getDataPath())
        .with("metadataPath", server.getType().getMetadataPath())
        .end();
  }

}
