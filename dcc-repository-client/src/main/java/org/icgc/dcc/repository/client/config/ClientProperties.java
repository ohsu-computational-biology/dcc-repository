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

import java.net.URI;
import java.util.Set;

import javax.validation.Valid;

import org.hibernate.validator.constraints.URL;
import org.icgc.dcc.repository.client.core.RepositoryImporter;
import org.icgc.dcc.repository.client.core.RepositoryImporter.Step;
import org.icgc.dcc.repository.client.util.MongoURI;
import org.icgc.dcc.repository.core.model.RepositorySource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;
import com.mongodb.MongoClientURI;

import lombok.Data;

@Data
@Component
@ConfigurationProperties
public class ClientProperties {

  @Valid
  RepositoryProperties repository;
  @Valid
  ImportsProperties imports;
  @Valid
  IdProperties id;
  MailProperties mail;

  @Data
  public static class RepositoryProperties {

    Set<RepositoryImporter.Step> steps = Sets.newHashSet();
    Set<RepositorySource> sources = Sets.newHashSet();

    @MongoURI
    MongoClientURI mongoUri;
    URI esUri;
    String indexAlias;

    public Set<RepositoryImporter.Step> getSteps() {
      return steps == null || steps.isEmpty() ? Step.all() : steps;
    }

    public Set<RepositorySource> getSources() {
      return sources == null || sources.isEmpty() ? RepositorySource.all() : sources;
    }

  }

  @Data
  public static class ImportsProperties {

    @MongoURI
    MongoClientURI mongoUri;

  }

  @Data
  public static class IdProperties {

    @URL
    String serviceUrl;
    String authToken;

  }

  @Data
  public static class MailProperties {

    boolean enabled;

  }

}
