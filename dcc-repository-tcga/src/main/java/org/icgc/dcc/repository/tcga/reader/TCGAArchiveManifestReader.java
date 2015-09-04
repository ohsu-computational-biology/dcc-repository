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
package org.icgc.dcc.repository.tcga.reader;

import static com.google.common.io.Resources.readLines;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.icgc.dcc.common.core.util.URLs.getUrl;

import java.io.IOException;
import java.util.List;

import org.icgc.dcc.repository.tcga.model.TCGAArchiveManifestEntry;

import lombok.SneakyThrows;
import lombok.val;

import com.google.common.collect.ImmutableList;

public class TCGAArchiveManifestReader {

  @SneakyThrows
  public static Iterable<TCGAArchiveManifestEntry> readEntries(String archiveUrl) {
    val entries = ImmutableList.<TCGAArchiveManifestEntry> builder();

    val lines = readManifest(archiveUrl);
    for (val line : lines) {
      String[] fields = parseFields(line);
      val md5 = fields[0];
      val fileName = fields[1];

      entries.add(new TCGAArchiveManifestEntry(md5, fileName));
    }

    return entries.build();
  }

  private static List<String> readManifest(String archiveUrl) throws IOException {
    val manifestUrl = getUrl(archiveUrl + "/MANIFEST.txt");
    return readLines(manifestUrl, UTF_8);
  }

  private static String[] parseFields(String line) {
    return line.split("\\s+");
  }

}