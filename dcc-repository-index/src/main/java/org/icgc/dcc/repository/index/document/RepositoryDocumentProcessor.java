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

import static java.util.stream.Collectors.toList;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.array;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.icgc.dcc.repository.core.model.Repositories;
import org.icgc.dcc.repository.core.model.Repositories.Repository;
import org.icgc.dcc.repository.core.model.RepositoryAccess;
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
    for (val repository : Repositories.getRepositories()) {
      val id = repository.getCode();
      val document = createDocument(repository);
      addDocument(id, document);
      count++;
    }

    return count;
  }

  private ObjectNode createDocument(Repository repository) {
    return object()
        .with("id", repository.getCode())
        .with("code", repository.getCode())
        .with("type", repository.getType().getId())
        .with("name", repository.getName())
        .with("source", repository.getSource().getId())
        .with("storage", repository.getStorage().getId())
        .with("environment", repository.getEnvironment().getId())
        .with("access", array().with(repository.getAccess().stream().map(RepositoryAccess::getId).collect(toList())))
        .with("country", repository.getCountry())
        .with("timezone", repository.getTimezone())
        .with("baseUrl", repository.getBaseUrl())
        .with("dataPath", repository.getType().getDataPath())
        .with("metadataPath", repository.getType().getMetadataPath())
        .end();
  }

}
