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
package org.icgc.dcc.repository.ega.util;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import org.icgc.dcc.repository.ega.model.EGASampleFile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

@NoArgsConstructor(access = PRIVATE)
public final class EGASampleFiles {

  public static JsonNode getSamples(@NonNull EGASampleFile sampleFile) {
    return at(sampleFile, "/SAMPLE_SET/SAMPLE");
  }

  public static String getSampleRefName(@NonNull JsonNode sampleRef) {
    return sampleRef.path("refname").textValue();
  }

  public static String getSampleAlias(@NonNull JsonNode sample) {
    return sample.get("alias").textValue();
  }

  public static ObjectNode getSampleAttributes(@NonNull JsonNode sample) {
    val attributes = DEFAULT.createObjectNode();
    val array = sample.at("/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE");
    for (val element : array) {
      attributes.put(element.get("TAG").textValue(), element.get("VALUE").textValue());
    }

    return attributes;
  }

  private static JsonNode at(EGASampleFile sampleFile, String path) {
    return sampleFile.getContents().at(path);
  }
}
