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
package org.icgc.dcc.repository.tcga.reader;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.regex.Pattern;

import org.icgc.dcc.repository.tcga.model.TCGAArchivePageEntry;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;

import lombok.SneakyThrows;
import lombok.Value;
import lombok.val;

public class TCGAArchivePageReader {

  /**
   * Regexes.
   */
  private static String LAST_MODIFIED_COLUMN_REGEX = "\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}";
  private static String SIZE_COLUMN_REGEX = "\\d+\\w+";
  private static String COLUMNS_REGEX =
      "\\s*(" + LAST_MODIFIED_COLUMN_REGEX + "|-)\\s*" + "(" + SIZE_COLUMN_REGEX + "|-)\\s*$";

  /**
   * Patterns.
   */
  private static Pattern COLUMNS_PATTERN = Pattern.compile(COLUMNS_REGEX);

  /**
   * Parsers.
   */
  private static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter
      .ofPattern("yyyy-MM-dd HH:mm")
      .withZone(ZoneId.systemDefault()); // Assume TCGA is on east coast

  @SneakyThrows
  public static Iterable<TCGAArchivePageEntry> readEntries(String archiveUrl) {
    val entries = ImmutableList.<TCGAArchivePageEntry> builder();

    String fileName = null;
    Columns columns = null;
    for (val node : getNodes(archiveUrl)) {
      if (isText(node)) {
        val text = ((TextNode) node).text();
        columns = parseColumns(text);

        if (!columns.isEmpty()) {
          val entry = new TCGAArchivePageEntry(fileName, columns.getSize(), columns.getLastModified());
          entries.add(entry);
        }
      } else if (isLink(node)) {
        fileName = node.attributes().get("href");
      }
    }

    return entries.build();
  }

  private static List<Node> getNodes(String archiveUrl) throws IOException {
    val document = Jsoup.connect(archiveUrl).get();
    val pre = document.select("pre").first();

    return pre.childNodes();
  }

  private static Columns parseColumns(String text) {
    val matcher = COLUMNS_PATTERN.matcher(text);

    return new Columns(
        matcher.matches() ? parseSize(matcher.group(2)) : null,
        matcher.matches() ? parseLastModified(matcher.group(1)) : null);
  }

  private static Long parseSize(String text) {
    if (text.equals("-")) {
      return null;
    }

    val number = Longs.tryParse(text);
    if (number != null) {
      return number;
    }

    val digits = Longs.tryParse(text.substring(0, text.length() - 1));
    val suffix = text.substring(text.length() - 1);
    val factor = suffix.equals("K") ? 1024 : -1;

    checkState(suffix.equals("K"), suffix);

    return digits * factor;
  }

  private static Instant parseLastModified(String text) {
    return ZonedDateTime.parse(text, DATE_FORMATTER).toInstant();
  }

  private static boolean isLink(Node node) {
    return node.nodeName().equals("a");
  }

  private static boolean isText(Node node) {
    return node instanceof TextNode;
  }

  @Value
  private static class Columns {

    Long size;
    Instant lastModified;

    public boolean isEmpty() {
      return lastModified == null && size == null;
    }

  }

}