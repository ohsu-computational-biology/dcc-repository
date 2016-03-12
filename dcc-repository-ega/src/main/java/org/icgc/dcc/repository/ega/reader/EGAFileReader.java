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
package org.icgc.dcc.repository.ega.reader;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.icgc.dcc.common.core.util.function.Predicates.not;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.json.XML;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import com.google.common.io.CharStreams;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

@RequiredArgsConstructor
public abstract class EGAFileReader<T> {

  /**
   * Constants.
   */
  private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JsonOrgModule());

  private static final Pattern TEST_FILE_PATTERN = Pattern.compile("TEST-PROJ.*");

  /**
   * Configuration.
   */
  @NonNull
  private final File repoDir;
  @NonNull
  private final Pattern filePattern;

  @SneakyThrows
  public List<T> readFiles() {
    return Files
        .walk(repoDir.toPath())
        .filter(not(this::isIgnored))
        .filter(this::isMatch)
        .map(this::createFile)
        .collect(toImmutableList());
  }

  protected abstract T createFile(Path path, Matcher matcher);

  @SneakyThrows
  protected ObjectNode readFile(Path path) {
    val file = path.toFile();
    val compressed = file.getName().endsWith(".gz");
    val fileStream = new FileInputStream(file);
    val inputStream = compressed ? new GZIPInputStream(fileStream) : fileStream;

    // Can't use jackson-dataformat-xml because of lack of repeating elements support, etc.
    val reader = new InputStreamReader(inputStream, UTF_8);
    val xml = CharStreams.toString(reader);
    val json = XML.toJSONObject(xml);

    return MAPPER.convertValue(json, ObjectNode.class);
  }

  private T createFile(Path path) {
    // Parse template
    val matcher = match(path, filePattern);
    checkState(matcher.find());

    return createFile(path, matcher);
  }

  private boolean isIgnored(Path path) {
    return match(path, TEST_FILE_PATTERN).matches();
  }

  private boolean isMatch(Path path) {
    return match(path, filePattern).matches();
  }

  private Matcher match(Path path, Pattern filePattern) {
    // Match without using the absolute portion of the path
    val relativePath = repoDir.toPath().relativize(path);
    return filePattern.matcher(relativePath.toString());
  }

}
