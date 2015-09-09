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
package org.icgc.dcc.repository.client.index;

import static org.icgc.dcc.repository.client.index.RepositoryFileIndex.INDEX_TYPE_FILE_NAME;
import static org.icgc.dcc.repository.client.index.RepositoryFileIndex.INDEX_TYPE_FILE_TEXT_NAME;

import java.util.List;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.icgc.dcc.repository.core.model.RepositoryFileCollection;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.mongodb.MongoClientURI;

import lombok.val;

public class FileDocumentProcessor extends DocumentProcessor {

  public FileDocumentProcessor(MongoClientURI mongoUri, String indexName, BulkProcessor processor) {
    super(mongoUri, indexName, processor);
  }

  @Override
  public int process() {
    return eachDocument(RepositoryFileCollection.FILE, file -> {
      String id = file.get("id").textValue();

      add(INDEX_TYPE_FILE_NAME, id, file);
      add(INDEX_TYPE_FILE_TEXT_NAME, id, createFileText(file, id));
    });
  }

  private ObjectNode createFileText(ObjectNode file, String id) {
    val fileText = createDocument();
    fileText.put("type", "file");
    fileText.put("id", id);
    fileText.putPOJO("file_name", childTextValues(file, "file_copies", "file_name"));
    fileText.putPOJO("donor_id", childTextValues(file, "donors", "donor_id"));

    return fileText;
  }

  private static List<String> childTextValues(ObjectNode objectNode, String parent, String child) {
    val textValues = Lists.<String> newArrayList();
    for (val element : objectNode.path(parent)) {
      val textValue = element.path(child).textValue();
      textValues.add(textValue);
    }

    return textValues;
  }

}
