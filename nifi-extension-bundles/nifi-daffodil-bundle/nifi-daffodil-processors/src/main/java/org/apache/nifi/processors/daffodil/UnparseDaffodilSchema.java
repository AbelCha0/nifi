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

import org.apache.daffodil.japi.ValidationMode;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.daffodil.dfdl.DfdlDataProcessor;
import org.apache.nifi.processors.daffodil.dfdl.MediaType;
import org.apache.nifi.util.StopWatch;
import java.io.File;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({
    "Daffodil", "DFDL", "unparse", "flat file", "fixed width", "delimited", "data transformation", "ETL", "XML", "JSON"
})
@CapabilityDescription(
    "Transforms structured XML or JSON data into flat file format using an Apache Daffodil DFDL schema. " +
    "This processor is useful for generating mainframe, legacy, or custom text data formats from standard XML/JSON. " +
    "The DFDL schema must define the output format. Supports configurable validation modes and output media types. " +
    "Typical use cases: generating EDI, COBOL copybook, or other custom text-based formats from XML/JSON."
)
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "Sets to the specified output format, default is text/plain."),
    @WritesAttribute(attribute = "daffodil.unparse.time.ms", description = "The time in milliseconds taken to unparse the input using Daffodil."),
    @WritesAttribute(attribute = "daffodil.unparse.status", description = "Indicates 'success' or 'failure' of the Daffodil unparse operation.")
})
@SeeAlso({ParseDaffodilSchema.class, UnparseEdi.class})
public class UnparseDaffodilSchema extends AbstractProcessor {

    private DfdlDataProcessor parser;
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private String inputMediaType;
    private String outputMediaType;

    public static final PropertyDescriptor DFDL_SCHEMA_FILE = new PropertyDescriptor
            .Builder().name("DFDL_SCHEMA_FILE")
            .displayName("DFDL Schema File")
            .description("Path to the DFDL schema file (.xsd) used to unparse the XML/JSON data into flat file format. The schema must be accessible to NiFi and define the structure and formatting rules for the output. Typical usage: provide an absolute or relative file path. Ensure the file is readable by the NiFi service user.")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor VALIDATION_MODE = new PropertyDescriptor
            .Builder().name("VALIDATION_MODE")
            .displayName("Validation Mode")
            .description("Specifies the level of validation to perform during unparsing. 'Off' disables validation, 'Limited' performs basic validation, and 'Full' enables comprehensive validation according to the DFDL schema. Use 'Full' for strict conformance, but it may impact performance.")
            .allowableValues(ValidationMode.Off.name(),ValidationMode.Limited.name(), ValidationMode.Full.name())
            .defaultValue(ValidationMode.Off.name())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INPUT_MEDIA_TYPE = new PropertyDescriptor
            .Builder().name("INPUT_MEDIA_TYPE")
            .displayName("Input Media Type")
            .description("Specifies the input format for the data to be unparsed. Choose 'XML' or 'JSON' according to your flow requirements.")
            .allowableValues(MediaType.XML.name(), MediaType.JSON.name())
            .defaultValue(MediaType.JSON.name())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor OUTPUT_MEDIA_TYPE = new PropertyDescriptor
            .Builder().name("OUTPUT_MEDIA_TYPE")
            .displayName("Output Media Type")
            .description("Specifies the output format for the unparsed data. Default is 'text/plain'. Downstream processors may expect a specific format, so select according to your flow requirements.")
            .defaultValue("text/plain")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully unparsed and transformed into flat file format according to the DFDL schema are routed to this relationship. The output content will be in the specified format, and relevant attributes will be updated.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that cannot be unparsed due to schema errors, validation failures, or processing exceptions are routed to this relationship. The original content is preserved for troubleshooting or reprocessing.")
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(DFDL_SCHEMA_FILE);
        descriptors.add(VALIDATION_MODE);
        descriptors.add(INPUT_MEDIA_TYPE);
        descriptors.add(OUTPUT_MEDIA_TYPE);
        descriptors = Collections.unmodifiableList(descriptors);
        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws Throwable {
        String schemaFilePath = context.getProperty(DFDL_SCHEMA_FILE).getValue();
        inputMediaType = context.getProperty(INPUT_MEDIA_TYPE).getValue();
        outputMediaType = context.getProperty(OUTPUT_MEDIA_TYPE).getValue();
        String validationMode = context.getProperty(VALIDATION_MODE).getValue();
        URI schemaUri = new File(schemaFilePath).toURI();
        parser = new DfdlDataProcessor(schemaUri, null, ValidationMode.valueOf(validationMode),false);
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }
        final StopWatch stopWatch = new StopWatch(true);
        try {
            final FlowFile transformed = session.write(original, (inputStream, outputStream) -> {
                try {
                    parser.unParse(inputStream, MediaType.valueOf(inputMediaType), outputStream);
                } catch (Throwable e) {
                    getLogger().error("Failed to unparse FlowFile (id={}) using Daffodil: {}", new Object[]{original.getId(), e.getMessage()}, e);
                    throw new ProcessException("DFDL Unparse Failed", e);
                }
            });
            session.putAttribute(transformed, "mime.type", outputMediaType);
            session.transfer(transformed, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(transformed, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            getLogger().info("Successfully unparsed FlowFile (id={}) to {} in {} ms", new Object[]{transformed.getId(), outputMediaType, stopWatch.getElapsed(TimeUnit.MILLISECONDS)});
        } catch (final Exception e) {
            getLogger().error("Failed to unparse FlowFile (id={}) due to {}", new Object[]{original.getId(), e.getMessage()}, e);
            session.transfer(original, REL_FAILURE);
        }
    }
}
