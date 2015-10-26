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
package org.icgc.dcc.repository.cghub.util;

import static lombok.AccessLevel.PRIVATE;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import lombok.NoArgsConstructor;

@NoArgsConstructor(access = PRIVATE)
public final class CGHubConverters {

  private static final Map<String, String> SAMPLE_TYPE_CODE_MAPPING = new HashMap<String, String>() { // Need nulls

    {
      put("01", "Primary tumour - solid tissue");
      put("02", "Recurrent tumour - solid tissue");
      put("03", "Primary tumour - blood derived (peripheral blood)");
      put("04", "Recurrent tumour - blood derived (bone marrow)");
      put("05", "Primary tumour - additional new primary");
      put("06", null); // See https://jira.oicr.on.ca/browse/DCC-4023
      put("07", "Metastatic tumour - additional metastatic");
      put("08", null); // See https://jira.oicr.on.ca/browse/DCC-4023
      put("09", "Primary tumour - blood derived (bone marrow)");
      put("10", "Normal - blood derived");
      put("11", "Normal - solid tissue");
      put("12", "Normal - buccal cell");
      put("13", "Normal - EBV immortalized");
      put("14", "Normal - bone marrow");
      put("20", null); // See https://jira.oicr.on.ca/browse/DCC-4023
      put("40", "Recurrent tumour - blood derived (peripheral blood)");
      put("50", "Cell line - derived from tumour");
      put("60", "Xenograft - derived from primary tumour");
      put("61", "Xenograft - derived from tumour cell line");
    }

  };

  private static final Map<String, String> ANALYTE_CODE_MAPPING = ImmutableMap.<String, String> builder()
      .put("D", "DNA")
      .put("G", "Whole Genome Amplification (WGA) produced using GenomePlex (Rubicon) DNA")
      .put("H", "mirVana RNA (Allprep DNA) produced by hybrid protocol")
      .put("R", "RNA")
      .put("T", "Total RNA")
      .put("W", "Whole Genome Amplification (WGA) produced using Repli-G (Qiagen) DNA")
      .put("X", "Whole Genome Amplification (WGA) produced using Repli-G X (Qiagen) DNA (2nd Reaction)")
      .build();

  public static String convertAnalyteCode(String code) {
    return ANALYTE_CODE_MAPPING.get(code);
  }

  public static String convertSampleTypeCode(String code) {
    return SAMPLE_TYPE_CODE_MAPPING.get(code);
  }

}
