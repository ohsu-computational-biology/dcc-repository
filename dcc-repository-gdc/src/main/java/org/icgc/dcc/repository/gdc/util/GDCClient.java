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
package org.icgc.dcc.repository.gdc.util;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;
import static org.icgc.dcc.common.core.util.Joiners.COMMA;

import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * GDC API client wrapper.
 * 
 * @see https://gdc-docs.nci.nih.gov/API/Users_Guide/Search_and_Retrieval/#files-endpoint
 */
@Slf4j
@RequiredArgsConstructor
public class GDCClient {

  /**
   * Constants
   */
  private static final String DEFAULT_API_URL = "https://gdc-api.nci.nih.gov";
  private static final int DEFAULT_PAGE_SIZE = 500;

  private static final int MAX_ATTEMPTS = 10;
  private static final int READ_TIMEOUT = (int) SECONDS.toMillis(60);

  private static final String APPLICATION_JSON = "application/json";

  public GDCClient() {
    this(DEFAULT_API_URL);
  }

  /**
   * Configuration.
   */
  @NonNull
  private final String url;

  public JsonNode getFilesMapping() {
    return getMapping("/files");
  }

  public List<ObjectNode> getFiles() {
    return getFiles(Query.builder().build());
  }

  public List<ObjectNode> getFiles(@NonNull Query query) {
    Pagination pagination = null;
  
    val results = ImmutableList.<ObjectNode> builder();
  
    int from = query.getFrom();
    int size = query.getSize();
  
    do {
      val response = readFiles(query, size, from);
      val hits = getHits(response);
      pagination = getPagination(response);
      log.info("{}", pagination);
  
      for (val hit : hits) {
        results.add((ObjectNode) hit);
      }
  
      from += size;
    } while (pagination.getPage() < pagination.getPages());
  
    val files = results.build();
    checkState(pagination.getTotal() == files.size(),
        "Pagination size (%s) not equal to files size (%s). There is a either a logic error or new files have been added while iterating",
        pagination.getCount(), files.size());
  
    return files;
  }

  public List<ObjectNode> getFilesPage(@NonNull Query query) {
    val response = readFiles(query, query.getSize(), query.getFrom());
    val hits = getHits(response);

    val results = ImmutableList.<ObjectNode> builder();
    for (val hit : hits) {
      results.add((ObjectNode) hit);
    }

    return results.build();
  }

  private JsonNode readFiles(Query query, int size, int from) {
    val params = Maps.<String, Object> newLinkedHashMap();

    params.put("size", size);
    params.put("from", from);
    if (query.getFields() != null) {
      params.put("fields", COMMA.join(query.getFields()));
    }
    if (query.getFilters() != null) {
      params.put("filters", query.getFilters());
    }
    if (query.getExpands() != null) {
      params.put("expand", COMMA.join(query.getExpands()));
    }

    val request = "/files" + "?" + Joiner.on('&').withKeyValueSeparator("=").join(params);

    int attempts = 0;
    while (++attempts <= MAX_ATTEMPTS) {
      try {
        val connection = openConnection(request);

        val response = readResponse(connection);
        checkWarnings(response);

        return response;
      } catch (SocketTimeoutException e) {
        log.warn("Socket timeout for {} after {} attempt(s)", request, attempts);
      }
    }

    throw new IllegalStateException("Could not get " + request);
  }

  @SneakyThrows
  private JsonNode getMapping(String path) {
    val connection = openConnection(path + "/_mapping");

    return readResponse(connection);
  }

  @SneakyThrows
  private HttpURLConnection openConnection(String path) throws SocketTimeoutException {
    val request = new URL(url + path);

    log.debug("Request: {}", request);
    val connection = (HttpsURLConnection) request.openConnection();
    connection.setRequestProperty(ACCEPT, APPLICATION_JSON);
    connection.setReadTimeout(READ_TIMEOUT);
    connection.setConnectTimeout(READ_TIMEOUT);

    return connection;
  }

  private static void checkWarnings(JsonNode response) {
    val warnings = response.path("warnings");
    if (warnings.size() > 0) {
      log.warn("Warnings: {}", warnings);
    }
  }

  @SneakyThrows
  private static JsonNode readResponse(URLConnection connection) {
    return DEFAULT.readTree(connection.getInputStream());
  }

  private static JsonNode getHits(JsonNode response) {
    return response.path("data").path("hits");
  }

  private static Pagination getPagination(JsonNode response) {
    return DEFAULT.convertValue(response.path("data").path("pagination"), Pagination.class);
  }

  @Value
  @Builder
  public static class Query {

    Integer from;
    Integer size;

    ObjectNode filters;
    List<String> expands;
    List<String> fields;

    public Integer getFrom() {
      return firstNonNull(from, 1);
    }

    public Integer getSize() {
      return firstNonNull(size, DEFAULT_PAGE_SIZE);
    }

  }

  @Data
  private static class Pagination {

    int count;
    String sort;
    int from;
    int page;
    int total;
    int pages;
    int size;

  }

}
