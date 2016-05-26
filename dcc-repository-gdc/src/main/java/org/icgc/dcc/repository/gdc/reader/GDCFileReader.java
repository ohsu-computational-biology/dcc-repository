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
package org.icgc.dcc.repository.gdc.reader;

import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.array;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.repository.gdc.util.GDCClient.Query.query;
import static org.icgc.dcc.repository.gdc.util.GDCProjects.getProjectsIds;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.icgc.dcc.repository.gdc.util.GDCClient;
import org.icgc.dcc.repository.gdc.util.GDCClient.Pagination;
import org.icgc.dcc.repository.gdc.util.GDCClient.Query;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * Reads filtered GDC file records with the appropriate set of fields for downstream processing.
 * 
 * @see https://wiki.oicr.on.ca/pages/viewpage.action?pageId=66946440
 */
@Slf4j
@RequiredArgsConstructor
public class GDCFileReader {

  /**
   * Constants.
   */
  private static final int PAGE_SIZE = 10000;

  private static final List<String> FIELD_NAMES =
      ImmutableList.of(
          "access",
          "state",
          "file_name",
          "data_type",
          "data_category",
          "md5sum",
          "updated_datetime",
          "data_format",
          "file_size",
          "file_id",
          "platform",
          "annotations.annotation_id",
          "archive.archive_id",
          "experimental_strategy",
          "center.name",
          "submitter_id",
          "cases.case_id",
          "cases.submitter_id",
          "cases.project.project_id",
          "cases.project.name",
          "cases.project.primary_site",
          "cases.samples.sample_type",
          "cases.samples.sample_id",
          "cases.samples.submitter_id",
          "cases.samples.portions.analytes.aliquots.aliquot_id",
          "cases.samples.portions.analytes.aliquots.submitter_id",
          "index_files.file_id",
          "index_files.data_format",
          "index_files.file_size",
          "index_files.file_name",
          "index_files.md5sum",
          "index_files.updated_datetime",
          "analysis.workflow_type",
          "analysis.analysis_id",
          "analysis.updated_datetime");

  private static ObjectNode PROJECT_FILTER =
      object()
          .with("op", "in")
          .with("content",
              object()
                  .with("field", "cases.project.project_id")
                  .with("value", array().with(getProjectsIds())))
          .end();

  /**
   * Dependencies.
   */
  @NonNull
  private final GDCClient client;

  public Stream<ObjectNode> readFiles() {
    val pages = new PageIterator(client,
        query()
            .size(PAGE_SIZE)
            .fields(FIELD_NAMES)
            .filters(PROJECT_FILTER)
            .build());

    return stream(pages).flatMap(List::stream);
  }

  private static <T> Stream<T> stream(Iterator<T> iterator) {
    return StreamSupport.stream(spliteratorUnknownSize(iterator, ORDERED), false);
  }

  private static class PageIterator implements Iterator<List<ObjectNode>> {

    /**
     * Dependencies.
     */
    private final GDCClient client;

    /**
     * Configuration.
     */
    private final Query query;

    /**
     * State.
     */
    private int from;
    private Pagination pagination;

    public PageIterator(@NonNull GDCClient client, @NonNull Query query) {
      this.client = client;
      this.query = query;
      this.from = query.getFrom();
    }

    @Override
    public boolean hasNext() {
      return hasNotStarted() || hasMorePages();
    }

    private boolean hasNotStarted() {
      return pagination == null;
    }

    private boolean hasMorePages() {
      return pagination.getPage() < pagination.getPages();
    }

    @Override
    public List<ObjectNode> next() {
      val page = client.getFiles(query.toBuilder().from(from).build());

      // Advance
      pagination = page.getPagination();
      from += query.getSize();

      log.info("{}", pagination);
      return page.getHits();
    }

  }

}
