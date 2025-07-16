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
import org.apache.daffodil.japi.io.InputSourceDataInputStream;
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
import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({
    "EDI", "DFDL", "parse", "X12", "EDIFact", "XML", "JSON", "data transformation", "flat file", "legacy integration"
})
@CapabilityDescription(
    "Parses incoming EDI files (such as X12 or EDIFact) using a DFDL schema, transforming EDI data into structured XML or JSON. " +
    "This processor is useful for integrating mainframe, B2B, or legacy EDI data into modern data flows. " +
    "The DFDL schema must define the EDI format. Supports configurable segment, element, and composite separators, as well as validation and encoding options. " +
    "Typical use cases: parsing EDI purchase orders, invoices, or other transactional messages."
)
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "Sets to 'application/xml' or 'application/json' based on the output media type."),
    @WritesAttribute(attribute = "edi.parse.time.ms", description = "The time in milliseconds taken to parse the input using Daffodil."),
    @WritesAttribute(attribute = "edi.parse.status", description = "Indicates 'success' or 'failure' of the EDI parse operation.")
})
@SeeAlso({UnparseEdi.class, ParseDaffodilSchema.class})
public class ParseEdi extends AbstractProcessor {
    private DfdlDataProcessor parser;
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private String dataElementSep;
    private  String mediaType;

    public static final PropertyDescriptor EDI_SCHEMA_FILE = new PropertyDescriptor
            .Builder().name("EDI_SCHEMA_FILE")
            .displayName("EDI Schema File")
            .description("Path to the DFDL schema file (.xsd) used to parse the incoming EDI data. The schema must be accessible to NiFi and define the structure and parsing rules for the EDI format. Typical usage: provide an absolute or relative file path. Ensure the file is readable by the NiFi service user.")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor SEGMENT_TERMINATOR = new PropertyDescriptor
            .Builder().name("SEGMENT_TERMINATOR")
            .displayName("Segment Terminator")
            .description("Specifies the character used to terminate segments in the EDI file (e.g., ' for UN/EDIFACT, ~ for X12). Must match the EDI data format.")
            .required(true)
            .defaultValue("'")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATA_ELEMENT_SEPARATOR = new PropertyDescriptor
            .Builder().name("DATA_ELEMENT_SEPARATOR")
            .displayName("Data Element Separator")
            .description("Specifies the character used to separate data elements within a segment (e.g., + for UN/EDIFACT, * for X12). Must match the EDI data format.")
            .required(true)
            .defaultValue("+")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor COMPOSITE_ELEMENT_SEPARATOR = new PropertyDescriptor
            .Builder().name("COMPOSITE_ELEMENT_SEPARATOR")
            .displayName("Composite Element Separator")
            .description("Specifies the character used to separate composite elements within a data element (e.g., : for UN/EDIFACT). Must match the EDI data format.")
            .required(true)
            .defaultValue(":")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ESCAPE_CHARACTER = new PropertyDescriptor
            .Builder().name("ESCAPE_CHARACTER")
            .displayName("Escape Character")
            .description("Specifies the escape character used in the EDI data, if any. Optional.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor EDI_ENCODING = new PropertyDescriptor
            .Builder().name("EDI_ENCODING")
            .displayName("EDI Encoding")
            .description("Specifies the character encoding of the EDI file (e.g., UTF-8, ISO-8859-1). Optional, defaults to UTF-8 if not set.")
            .required(false)
            .build();

    public static final PropertyDescriptor VALIDATION_MODE = new PropertyDescriptor
            .Builder().name("VALIDATION_MODE")
            .displayName("Validation Mode")
            .description("Specifies the level of validation to perform during parsing. 'Off' disables validation, 'Limited' performs basic validation, and 'Full' enables comprehensive validation according to the DFDL schema. Use 'Full' for strict conformance, but it may impact performance.")
            .allowableValues(ValidationMode.Off.name(),ValidationMode.Limited.name(), ValidationMode.Full.name())
            .defaultValue(ValidationMode.Off.name())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MEDIA_TYPE = new PropertyDescriptor
            .Builder().name("MEDIA_TYPE")
            .displayName("Output Media Type")
            .description("Specifies the output format for parsed data. Choose 'XML' to output standard XML, or 'JSON' for JSON format. Downstream processors may expect a specific format, so select according to your flow requirements.")
            .allowableValues(MediaType.XML.name(), MediaType.JSON.name())
            .defaultValue(MediaType.JSON.name())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully parsed and transformed according to the DFDL schema are routed to this relationship. The output content will be in the selected media type (XML or JSON), and relevant attributes will be updated.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that cannot be parsed due to schema errors, validation failures, or processing exceptions are routed to this relationship. The original content is preserved for troubleshooting or reprocessing.")
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(EDI_SCHEMA_FILE);
        descriptors.add(SEGMENT_TERMINATOR);
        descriptors.add(DATA_ELEMENT_SEPARATOR);
        descriptors.add(COMPOSITE_ELEMENT_SEPARATOR);
        descriptors.add(ESCAPE_CHARACTER);
        descriptors.add(MEDIA_TYPE);
        descriptors.add(EDI_ENCODING);
        descriptors.add(VALIDATION_MODE);
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
        String ediSchemaFilePath = context.getProperty(EDI_SCHEMA_FILE).getValue();
        String segmentTerminator = context.getProperty(SEGMENT_TERMINATOR).getValue();
        dataElementSep = context.getProperty(DATA_ELEMENT_SEPARATOR).getValue();
        String compElementSep = context.getProperty(COMPOSITE_ELEMENT_SEPARATOR).getValue();
        mediaType = context.getProperty(MEDIA_TYPE).getValue();
        String encoding =  context.getProperty(EDI_ENCODING).getValue();
        if(null == encoding || encoding.isEmpty()){
            encoding = "UTF-8";
        }
        String validationMode = context.getProperty(VALIDATION_MODE).getValue();
        URI schemaUri = new File(ediSchemaFilePath).toURI();

        String finalEncoding = encoding;
        HashMap<String, String> variables =new HashMap<String, String>() {{
            this.put("dfdl:encoding", finalEncoding);
            this.put("{http://www.ibm.com/dfdl/EDI/Format}SegmentTerm", segmentTerminator);
            this.put("{http://www.ibm.com/dfdl/EDI/Format}FieldSep", dataElementSep);
            this.put("{http://www.ibm.com/dfdl/EDI/Format}CompositeSep", compElementSep);
            if(context.getProperty(ESCAPE_CHARACTER).isSet()) {
                this.put("{http://www.ibm.com/dfdl/EDI/Format}EscapeChar", context.getProperty(ESCAPE_CHARACTER).getValue());
            }
        }};
        parser = new DfdlDataProcessor(schemaUri, variables, ValidationMode.valueOf(validationMode),false);
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session){
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }
        final StopWatch stopWatch = new StopWatch(true);
        try {
            final FlowFile transformed = session.write(original, (inputStream, outputStream) -> {
                try (final InputStream bufferedInputStream = new BufferedInputStream(inputStream)) {
                    InputSourceDataInputStream dis = new InputSourceDataInputStream(bufferedInputStream);
                    parser.parse(dis,MediaType.valueOf(mediaType),outputStream);
                } catch (final Throwable e) {
                    getLogger().error("Failed to parse FlowFile (id={}) using Daffodil: {}", new Object[]{original.getId(), e.getMessage()}, e);
                    throw new ProcessException("EDI Transform Failed", e);
                }
            });
            session.putAttribute(transformed,"mime.type", mediaType.equals(MediaType.JSON.name())?"application/json":"application/xml");
            session.transfer(transformed, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(transformed, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            getLogger().info("Successfully transformed FlowFile (id={}) to {} in {} ms", new Object[]{transformed.getId(), mediaType, stopWatch.getElapsed(TimeUnit.MILLISECONDS)});
        } catch (final Exception e) {
            getLogger().error("Failed to transform FlowFile (id={}) due to {}", new Object[]{original.getId(), e.getMessage()}, e);
            session.transfer(original, REL_FAILURE);
        }
    }
}