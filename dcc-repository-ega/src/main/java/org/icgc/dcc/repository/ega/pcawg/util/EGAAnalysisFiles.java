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
package org.icgc.dcc.repository.ega.pcawg.util;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.json.Jackson.DEFAULT;

import org.icgc.dcc.repository.ega.pcawg.model.EGAAnalysisFile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

@NoArgsConstructor(access = PRIVATE)
public final class EGAAnalysisFiles {

  public static ObjectNode getReferenceAssembly(@NonNull EGAAnalysisFile analysisFile) {
    return (ObjectNode) at(analysisFile, "/ANALYSIS_SET/ANALYSIS/ANALYSIS_TYPE/REFERENCE_ALIGNMENT/ASSEMBLY/CUSTOM");
  }

  public static ArrayNode getFiles(@NonNull EGAAnalysisFile analysisFile) {
    return (ArrayNode) at(analysisFile, "/ANALYSIS_SET/ANALYSIS/FILES/FILE");
  }

  public static String getFileName(@NonNull JsonNode file) {
    return file.get("filename").textValue();
  }

  public static String getFileType(@NonNull JsonNode file) {
    return file.get("filetype").textValue();
  }

  public static String getChecksum(@NonNull JsonNode file) {
    return file.get("checksum").textValue();
  }

  public static ObjectNode getAnalysisAttributes(@NonNull EGAAnalysisFile analysisFile) {
    val attributes = DEFAULT.createObjectNode();
    val array = at(analysisFile, "/ANALYSIS_SET/ANALYSIS/ANALYSIS_ATTRIBUTES/ANALYSIS_ATTRIBUTE");
    for (val element : array) {
      attributes.put(element.get("TAG").textValue(), element.get("VALUE").textValue());
    }

    return attributes;
  }

  public static JsonNode getSampleRef(@NonNull EGAAnalysisFile analysisFile) {
    return at(analysisFile, "/ANALYSIS_SET/ANALYSIS/SAMPLE_REF");
  }

  private static JsonNode at(EGAAnalysisFile analysisFile, String path) {
    return analysisFile.getContents().at(path);
  }

}
