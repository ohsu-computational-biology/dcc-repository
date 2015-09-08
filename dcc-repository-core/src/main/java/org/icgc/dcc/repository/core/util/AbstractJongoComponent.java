/*
 * Copyright (c) 2014 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.repository.core.util;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.repository.core.util.Jongos.newJongo;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.icgc.dcc.repository.core.model.RepositoryFileCollection;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClientURI;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractJongoComponent implements Closeable {

  /**
   * Configuration.
   */
  protected final MongoClientURI mongoUri;

  /**
   * Dependencies.
   */
  protected final Jongo jongo;

  public AbstractJongoComponent(@NonNull MongoClientURI mongoUri) {
    this.mongoUri = mongoUri;
    this.jongo = newJongo(mongoUri);
  }

  @Override
  public void close() throws IOException {
    jongo.getDatabase().getMongo().close();
  }

  protected MongoCollection getCollection(@NonNull RepositoryFileCollection fileCollection) {
    return getCollection(fileCollection.getId());
  }

  protected MongoCollection getCollection(@NonNull String collectionName) {
    return jongo.getCollection(collectionName);
  }

  protected MongoCursor<ObjectNode> readDocuments(@NonNull RepositoryFileCollection fileCollection) {
    return readDocuments(fileCollection.getId());
  }

  protected MongoCursor<ObjectNode> readDocuments(@NonNull String collectionName) {
    return getCollection(collectionName).find().as(ObjectNode.class);
  }

  protected void clearDocuments(@NonNull RepositoryFileCollection fileCollection) {
    val collection = getCollection(fileCollection);

    log.info("Clearing documents in collection '{}'", collection.getName());
    val result = collection.remove();
    checkState(result.getLastError().ok(), "Error clearing mongo: %s", result);

    log.info("Finished clearing {} documents in collection '{}'",
        formatCount(result.getN()), collection.getName());
  }

  protected int eachDocument(@NonNull RepositoryFileCollection fileCollection, @NonNull Consumer<ObjectNode> handler) {
    int documentCount = 0;
    for (val document : readDocuments(fileCollection)) {
      handler.accept(document);

      documentCount++;
    }

    return documentCount;
  }

  protected int eachDocument(@NonNull String collectionName, @NonNull Consumer<ObjectNode> handler) {
    int documentCount = 0;
    for (val document : readDocuments(collectionName)) {
      handler.accept(document);

      documentCount++;
    }

    return documentCount;
  }

  protected <T> List<T> mapDocument(@NonNull RepositoryFileCollection fileCollection,
      @NonNull Function<ObjectNode, T> mapping) {
    val results = ImmutableList.<T> builder();
    for (val document : readDocuments(fileCollection)) {
      val result = mapping.apply(document);

      results.add(result);
    }

    return results.build();
  }

}
