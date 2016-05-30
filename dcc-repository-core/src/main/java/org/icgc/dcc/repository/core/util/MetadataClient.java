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
package org.icgc.dcc.repository.core.util;

import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

/**
 * Responsible for interacting with metadata service.
 */
@RequiredArgsConstructor
public class MetadataClient {

  /**
   * Constants.
   */
  private static final String DEFAULT_SERVER_URL = "https://meta.icgc.org";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Configuration.
   */
  @NonNull
  @Getter
  private final String serverUrl;

  public MetadataClient() {
    this(DEFAULT_SERVER_URL);
  }

  @SneakyThrows
  public Optional<Entity> findEntity(@NonNull String objectId) {
    Entity entity = null;
    try {
      entity = MAPPER.readValue(resolveUrl("/" + objectId), Entity.class);
    } catch (FileNotFoundException e) {
      // No-op
    }

    return Optional.ofNullable(entity);
  }

  @SneakyThrows
  private URL resolveUrl(String path) {
    return new URL(serverUrl + "/entities" + path);
  }

  @Data
  @EqualsAndHashCode(of = "id")
  public static class Entity {

    /**
     * Uniqueness.
     */
    String id;

    /**
     * Metadata.
     */
    String fileName;
    String gnosId;
    long createdTime;

  }

}
