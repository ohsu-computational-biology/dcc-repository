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

import static com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS;
import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Strings.padEnd;
import static org.apache.commons.lang.StringUtils.repeat;
import static org.icgc.dcc.common.core.util.VersionUtils.getScmInfo;

import java.util.Map;

import javax.annotation.PostConstruct;

import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired) )
public class ClientBanner {

  /**
   * Dependencies.
   */
  @NonNull
  private final ClientProperties properties;
  @NonNull
  private final StandardEnvironment env;

  @PostConstruct
  public void log() {
    log.info("{}", line());
    log.info("Version: {}", getVersion());
    log.info("Built:   {}", getBuildTimestamp());
    log.info("SCM:");
    log(getScmInfo());
    log(properties);
    log(env);
    log.info("{}\n\n", line());
  }

  private static void log(Object values) {
    val name = values.getClass().getName();

    log.info("{}:", name);
    log(convert(values));
  }

  private static void log(Map<String, ?> values) {
    for (val entry : convert(values).entrySet()) {
      val key = entry.getKey();
      val text = entry.getValue() == null ? null : entry.getValue().toString();
      val value = firstNonNull(text, "").replaceAll("\n", " ");

      log.info("         {}: {}", padEnd(key, 24, ' '), value);
    }
  }

  private static void log(StandardEnvironment env) {
    log.info("{}:", env);
    for (val source : env.getPropertySources()) {
      log.info("         {}:", source.getName());
      if (source instanceof SimpleCommandLinePropertySource) {
        val simple = (SimpleCommandLinePropertySource) source;
        for (val propertyName : simple.getPropertyNames()) {
          log.info("            - {}: {}", propertyName, simple.getProperty(propertyName));
        }
      } else if (source instanceof MapPropertySource) {
        val map = (MapPropertySource) source;
        for (val propertyName : Sets.newTreeSet(ImmutableSet.copyOf(map.getPropertyNames()))) {
          log.info("            - {}: {}", propertyName, map.getProperty(propertyName));
        }
      } else if (source instanceof EnumerablePropertySource) {
        val enumerable = (EnumerablePropertySource<?>) source;
        for (val propertyName : enumerable.getPropertyNames()) {
          log.info("            - {}: {}", propertyName, enumerable.getProperty(propertyName));
        }
      }
    }
  }

  private static Map<String, Object> convert(Object values) {
    return new ObjectMapper().configure(FAIL_ON_EMPTY_BEANS, false).convertValue(values,
        new TypeReference<Map<String, Object>>() {});
  }

  private static String line() {
    return repeat("-", 100);
  }

  private static String getVersion() {
    return firstNonNull(getPackage().getImplementationVersion(), "[unknown version]");
  }

  private static String getBuildTimestamp() {
    return firstNonNull(getPackage().getSpecificationVersion(), "[unknown build timestamp]");
  }

  private static Package getPackage() {
    return ClientConfig.class.getPackage();
  }

}