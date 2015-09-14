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
package org.icgc.dcc.repository.client.config;

import static org.icgc.dcc.repository.core.model.RepositorySource.all;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.icgc.dcc.repository.core.model.RepositorySource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import lombok.Data;
import lombok.val;

@Data
@Component
@ConfigurationProperties
public class ClientProperties {

  RepositoryProperties repository;
  ImportsProperties imports;
  IdProperties id;

  @PostConstruct
  public void validate() {
    validate(repository.getMongoUri());
    validate(imports.getMongoUri());
  }

  private void validate(MongoClientURI mongoUri) {
    try {
      val mongo = new MongoClient(mongoUri);
      try {
        // Test connectivity
        val socket = mongo.getMongoOptions().socketFactory.createSocket();
        socket.connect(mongo.getAddress().getSocketAddress());

        // All good
        socket.close();
      } catch (IOException ex) {
        new RuntimeException(mongoUri + " is not accessible", ex);
      } finally {
        mongo.close();
      }
    } catch (UnknownHostException e) {
      new RuntimeException(mongoUri + " host IP address could not be determined.", e);
    }
  }

  @Data
  public static class RepositoryProperties {

    Set<RepositorySource> sources;
    MongoClientURI mongoUri;
    String esUri;

    public Set<RepositorySource> getSources() {
      return sources == null || sources.isEmpty() ? all() : sources;
    }

  }

  @Data
  public static class ImportsProperties {

    MongoClientURI mongoUri;

  }

  @Data
  public static class IdProperties {

    String serviceUrl;
    String authToken;

  }

}
