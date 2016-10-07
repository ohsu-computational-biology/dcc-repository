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
package org.icgc.dcc.repository.exacloud.reader;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

import org.icgc.dcc.repository.exacloud.model.ExacloudArchiveListEntry;

import java.net.URL;
import java.util.List;

import lombok.SneakyThrows;
import lombok.val;

import static com.google.common.collect.Iterables.skip;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.icgc.dcc.common.core.util.Splitters.TAB;
import static org.icgc.dcc.common.core.util.URLs.getUrl;

public class ExacloudArchiveListReader {

  private static final URL EXACLOUD_ARCHIVE_LISTING =
          getUrl(System.getenv().get("EXACLOUD_ARCHIVE_LISTING"));
      // getUrl("file:///Users/walsbr/dcc-repository/dcc-repository-exacloud/src/test/resources/lls_resource.tsv");



  public static Iterable<ExacloudArchiveListEntry> readEntries() {
    val entries = ImmutableList.<ExacloudArchiveListEntry> builder();

    entries.add(new ExacloudArchiveListEntry("BAML", "06-10-2016", EXACLOUD_ARCHIVE_LISTING.toString()));

//    val lines = readLines();
//    for (val line : lines) {
//      val fields = parseFields(line);
//      val archiveName = fields.get(0);
//      val dateAdded = fields.get(1);
//      val archiveUrl = fields.get(2);
//
//      entries.add(new ExacloudArchiveListEntry(archiveName, dateAdded, archiveUrl));
//    }

    return entries.build();
  }

  @SneakyThrows
  private static Iterable<String> readLines() {
    // Skip header
    val headerLineCount = 1;
    return skip(Resources.readLines(EXACLOUD_ARCHIVE_LISTING, UTF_8), headerLineCount);
  }

  private static List<String> parseFields(String line) {
    return TAB.splitToList(line);
  }

}