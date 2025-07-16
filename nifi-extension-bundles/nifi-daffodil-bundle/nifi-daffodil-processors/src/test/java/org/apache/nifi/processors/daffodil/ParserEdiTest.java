/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.daffodil;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParserEdiTest {

    private TestRunner runner = TestRunners.newTestRunner(ParseEdi.class);

    @Test
    public void edi850ToJson() {
        runner.setProperty("EDI_SCHEMA_FILE", "./src/test/resources/X12_4010_850.xsd");
        runner.setProperty("SEGMENT_TERMINATOR", "~");
        runner.setProperty("DATA_ELEMENT_SEPARATOR", "*");
        runner.setProperty("COMPOSITE_ELEMENT_SEPARATOR", "'");
        runner.setProperty("MEDIA_TYPE", "JSON");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "850.edi");
        runner.enqueue(this.getClass().getClassLoader().getResourceAsStream("X12_4010_850.edi"), attributes);
        runner.run();
        List<MockFlowFile> results = runner.getFlowFilesForRelationship("success");
        Relationship expectedRel = ParseEdi.REL_SUCCESS;
        runner.assertTransferCount(expectedRel, 1);
        MockFlowFile result = results.get(0);
        assert result.getAttribute("mime.type").equals("application/json");
    }

    @Test
    public void edi850ToJsonError() {
        runner.setProperty("EDI_SCHEMA_FILE", "./src/test/resources/X12_4010_850.xsd");
        runner.setProperty("SEGMENT_TERMINATOR", "~");
        runner.setProperty("DATA_ELEMENT_SEPARATOR", "*");
        runner.setProperty("COMPOSITE_ELEMENT_SEPARATOR", "'");
        runner.setProperty("MEDIA_TYPE", "JSON");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "850.edi");
        runner.enqueue(this.getClass().getClassLoader().getResourceAsStream("X12_4010_850_WrongTranstCount.edi"), attributes);
        runner.run();
        List<MockFlowFile> results = runner.getFlowFilesForRelationship("success");
        Relationship expectedRel = ParseEdi.REL_FAILURE;
        runner.assertTransferCount(expectedRel, 1);
    }

    @Test
    public void desadvToJson() {
        runner.setProperty("EDI_SCHEMA_FILE", "./src/test/resources/EANCOM_96A_DESADV.xsd");
        runner.setProperty("SEGMENT_TERMINATOR", "'%WSP*; %NL;%WSP*;");
        runner.setProperty("DATA_ELEMENT_SEPARATOR", "+");
        runner.setProperty("COMPOSITE_ELEMENT_SEPARATOR", ":");
        runner.setProperty("ESCAPE_CHARACTER", "?");
        runner.setProperty("MEDIA_TYPE", "JSON");
        Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "desadv.edi");
        runner.enqueue(this.getClass().getClassLoader().getResourceAsStream("EANCOM_96A_DESADV.edi"), attributes);
        runner.run();
        List<MockFlowFile> results = runner.getFlowFilesForRelationship("success");
        Relationship expectedRel = ParseEdi.REL_SUCCESS;
        runner.assertTransferCount(expectedRel, 1);
        MockFlowFile result = results.get(0);
        assert result.getAttribute("mime.type").equals("application/json");
    }

}