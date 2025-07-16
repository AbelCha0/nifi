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

import org.apache.commons.io.FileUtils;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class UnparseDaffodilSchemaTest {

    private TestRunner runner = TestRunners.newTestRunner(UnparseDaffodilSchema.class);

    @BeforeAll
    public static void setUp() {
        System.setProperty("skipLicenseCheck","true");
    }

    @Test
    public void unParseCSVTest() throws IOException {
        runner.setProperty("DFDL_SCHEMA_FILE","./src/test/resources/csv.dfdl.xsd");
        runner.setProperty("INPUT_MEDIA_TYPE","XML");
        String payload = FileUtils.readFileToString(new File("./src/test/resources/token.csv.xml"), "utf-8");
        runner.enqueue(payload);
        runner.run();
        List<MockFlowFile> results = runner.getFlowFilesForRelationship("success");
        Relationship expectedRel = UnparseDaffodilSchema.REL_SUCCESS;
        runner.assertTransferCount(expectedRel, 1);
        MockFlowFile result = results.get(0);
        assert "text/plain".equals(result.getAttribute("mime.type"));
        System.out.println(result.getContent());
    }
}
