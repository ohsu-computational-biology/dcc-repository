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

import static org.icgc.dcc.repository.core.util.Jongos.newJongo;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.icgc.dcc.common.core.model.ReleaseCollection;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClientURI;

import lombok.NonNull;
import lombok.val;

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

  protected MongoCollection getCollection(@NonNull ReleaseCollection releaseCollection) {
    return jongo.getCollection(releaseCollection.getId());
  }

  protected MongoCursor<ObjectNode> readDocuments(ReleaseCollection collection) {
    return getCollection(collection).find().as(ObjectNode.class);
  }

  protected int eachDocument(@NonNull ReleaseCollection collection, @NonNull Consumer<ObjectNode> handler) {
    int documentCount = 0;
    for (val document : readDocuments(collection)) {
      handler.accept(document);

      documentCount++;
    }

    return documentCount;
  }

  protected <T> List<T> mapDocument(@NonNull ReleaseCollection collection, @NonNull Function<ObjectNode, T> mapping) {
    val results = ImmutableList.<T> builder();
    for (val document : readDocuments(collection)) {
      val result = mapping.apply(document);

      results.add(result);
    }

    return results.build();
  }

}
