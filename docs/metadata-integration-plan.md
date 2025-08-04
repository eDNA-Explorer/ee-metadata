# Input Metadata Integration Plan

## Overview
Add functionality to accept an input metadata CSV file and intelligently merge it with the generated FASTQ metadata, matching samples and normalizing data formats.

## 1. CLI Enhancement

### 1.1 Add Input Metadata Option
- Add `--input-metadata` / `-m` CLI option
- Make it optional with interactive prompt if not provided
- Support tab completion for file paths
- Validate file exists and is readable
- Support metadata-only mode when no FASTQ samples are present

### 1.2 Interactive Prompts
- If no input metadata provided, ask user if they want to specify one
- Allow users to skip this step entirely
- Provide clear guidance on expected format

## 2. Column Detection and Mapping

### 2.1 Intelligent Column Detection
Create fuzzy matching algorithm to detect columns:

**Site Name Detection:**
- Look for columns containing: `site`, `location`, `station`, `area`, `region`
- Case-insensitive matching
- Support variations like `site_name`, `Site Name`, `SITE`, etc.

**Sample Name Detection:**
- Look for columns containing: `sample`, `sample_id`, `specimen`, `id`, `name`
- Prioritize columns with `sample` keyword
- Support variations like `sample_name`, `Sample ID`, `SAMPLE`, etc.

**Sample Date Detection:**
- Look for columns containing: `date`, `collected`, `sampling`, `time`
- Support variations like `collection_date`, `Date Collected`, `SAMPLING_DATE`

**Latitude Detection:**
- Look for columns containing: `lat`, `latitude`, `y`, `coord`
- Support variations like `lat_dd`, `Latitude`, `LAT`, `decimal_lat`

**Longitude Detection:**
- Look for columns containing: `lon`, `long`, `longitude`, `x`, `coord`
- Support variations like `lon_dd`, `Longitude`, `LONG`, `decimal_lon`

**Sample Type Detection:**
- Look for columns containing: `type`, `sample_type`, `control`, `reference`, `project`
- Support variations like `Sample Type`, `SAMPLE_TYPE`, `Control`, `Reference Site`, `Project Site`
- Detect patterns indicating control vs sample classification

### 2.2 Interactive Column Mapping
- Display detected columns to user for confirmation
- Allow manual selection if auto-detection fails
- Show preview of data for each detected column
- Option to skip columns if not present in input file

## 3. Sample Name Matching

### 3.1 Fuzzy Matching Algorithm
Implement intelligent sample name matching:

**Preprocessing:**
- Remove common suffixes: `_R1`, `_R2`, `_001`, `_L001`
- Remove file extensions: `.fastq.gz`, `.fq.gz`
- Normalize case (convert to lowercase for matching)
- Remove special characters and extra whitespace

**Matching Strategy:**
1. **Exact match** (after preprocessing)
2. **Substring match** (sample name contains metadata name or vice versa)
3. **Fuzzy string matching** using Levenshtein distance (threshold ~80% similarity)
4. **Common prefix matching** (for systematic naming schemes)

**Multiple Matches Handling:**
- If multiple metadata rows match one FASTQ sample, present options to user
- If multiple FASTQ samples match one metadata row, flag for review
- Log all matching decisions for user review

### 3.2 Interactive Match Review
- Show matching results in a table format
- Allow users to manually override automatic matches
- Highlight uncertain matches (low confidence scores)
- Option to skip unmatched samples

## 4. Data Normalization

### 4.1 Date Normalization
Support multiple input date formats and convert to `YYYY-MM-DD`:

**Input Formats to Support:**
- `MM/DD/YYYY`, `DD/MM/YYYY` (detect based on values)
- `MM-DD-YYYY`, `DD-MM-YYYY`
- `YYYY/MM/DD`, `YYYY-MM-DD`
- `Month DD, YYYY` (e.g., "January 15, 2023")
- `DD-MMM-YYYY` (e.g., "15-Jan-2023")
- ISO formats with time: `YYYY-MM-DDTHH:MM:SS`

**Implementation:**
- Use Python's `dateutil.parser` for flexible parsing
- If ambiguous format (like 01/02/2023), prompt user for clarification
- Handle invalid dates gracefully with warnings
- Support partial dates (year only, year-month only)

### 4.2 Coordinate Normalization
Convert various coordinate formats to decimal degrees:

**Input Formats to Support:**
- Decimal degrees: `45.123456`
- Degrees with cardinal directions: `45.123456 N`, `45°7'24.4"N`
- Degrees, minutes, seconds: `45°7'24.4"`, `45 7 24.4`
- Mixed formats in single field: `45°7'24.4"N 123°8'45.2"W`

**Implementation:**
- Parse DMS (Degrees, Minutes, Seconds) format
- Handle cardinal directions (N/S for latitude, E/W for longitude)
- Validate coordinate ranges (-90 to 90 for lat, -180 to 180 for lon)
- Flag suspicious coordinates for review

### 4.3 Sample Type Normalization
Convert various sample type indicators to boolean (Sample vs Control):

**Control Indicators to Detect:**
- Explicit terms: `control`, `ctrl`, `reference`, `ref`, `blank`, `negative`
- Site-based: `reference site`, `project site`, `control site`
- Coded values: `C`, `R`, `CTRL`, `REF`, `NEG`
- Descriptive: `field blank`, `extraction blank`, `pcr blank`

**Sample Indicators to Detect:**
- Explicit terms: `sample`, `field`, `environmental`, `specimen`
- Site-based: `sample site`, `study site`, `field site`
- Coded values: `S`, `F`, `E`, `SAMPLE`, `FIELD`
- Default assumption: if not clearly a control, treat as sample

**Implementation:**
- Case-insensitive keyword matching
- Support for partial matches in longer descriptions
- Interactive confirmation for ambiguous cases
- Option to manually classify unclear entries
- Generate boolean output: `True` for Sample, `False` for Control

## 5. Metadata Integration

### 5.1 Data Merging Strategy
The tool needs to handle three distinct scenarios:

**Scenario 1: FASTQ Samples + Input Metadata (Standard Mode)**
- Merge matched metadata into the FASTQ-derived metadata
- Preserve all existing columns from FASTQ analysis
- Add new columns from input metadata
- Handle missing data gracefully (empty strings for unmatched samples)

**Scenario 2: FASTQ Samples Only (Current Behavior)**
- Generate metadata from FASTQ analysis only
- User selects detected markers interactively
- No external metadata integration

**Scenario 3: Input Metadata Only (Metadata-Only Mode)**
- No FASTQ samples provided in input directory
- Convert and standardize existing metadata CSV
- Prompt user for project markers to populate marker columns
- Generate standardized metadata format without FASTQ analysis

### 5.2 Comprehensive Sample Handling
**Unmatched FASTQ Samples:**
- FASTQ samples with no corresponding metadata entry
- Create new rows with FASTQ-derived data and empty metadata fields
- Clearly mark as "unmatched" in processing logs

**Unmatched Metadata Entries:**
- Metadata entries with no corresponding FASTQ samples
- Include these as additional rows in final output
- Mark FASTQ filename columns as empty
- Preserve all metadata information

**Perfect Matches:**
- Samples that exist in both FASTQ analysis and input metadata
- Merge all available information
- Highest data completeness

### 5.3 Metadata-Only Mode Implementation
**When No FASTQ Samples Found:**
1. Detect and inform user that no .fastq.gz files were found
2. Ask if user wants to proceed in metadata-only mode
3. If yes, prompt for project markers to include

**Manual Marker Entry Process:**
- Display available markers from primers.csv file
- Show markers in organized categories (by marker type/organism)
- Allow user to select relevant markers for their project
- Support multi-select interface (numbered selection like current marker confirmation)
- Populate selected marker columns with primer sequences from primers.csv

**Marker Column Population:**
- Use primer ID as marker name (from primers.csv `id` column)
- Populate ForwardPS and ReversePS from primers.csv sequences
- Apply consistent marker positioning (Marker 1, Marker 2, etc.)
- All metadata rows get all selected markers populated

### 5.4 Output Enhancement
- Include matching confidence scores in a separate log file
- Add metadata source tracking (which fields came from input vs. FASTQ analysis)
- Generate summary report of matching success rates
- Add standardized "Sample Type" column with boolean values (Sample/Control)
- Include sample type classification confidence in logs
- Clearly indicate processing mode (FASTQ+Metadata, FASTQ-only, Metadata-only)
- Summary statistics for each processing scenario

## 6. Error Handling and Validation

### 6.1 Input Validation
- Verify CSV file format and readability
- Check for required headers
- Validate data types in detected columns
- Handle empty or malformed CSV files

### 6.2 User Feedback
- Progress bars for large datasets
- Clear error messages with suggestions
- Summary statistics (matches found, data normalized, etc.)
- Option to export matching log for review

## 7. Implementation Steps

### Phase 1: Basic Infrastructure
1. Add CLI option for input metadata
2. Implement CSV reading and basic validation
3. Create column detection algorithms
4. Add interactive column mapping interface

### Phase 2: Sample Matching
1. Implement sample name preprocessing
2. Create fuzzy matching algorithms
3. Build interactive match review interface
4. Add confidence scoring system

### Phase 3: Data Normalization
1. Implement date parsing and normalization
2. Create coordinate conversion functions
3. Build sample type classification system
4. Add validation and error handling
5. Build user feedback for ambiguous cases

### Phase 4: Integration and Polish
1. Integrate metadata merging with existing workflow
2. Implement comprehensive sample handling (matched/unmatched)
3. Add metadata-only mode functionality
4. Add comprehensive error handling
5. Create detailed logging and reporting
6. Add unit tests for all new functionality

## 8. Example Workflows

### 8.1 Standard Mode (FASTQ + Metadata)
```bash
# With input metadata specified
poetry run ee-metadata ./fastq_data --input-metadata ./field_data.csv

# Interactive mode
poetry run ee-metadata
# Prompts for FASTQ directory
# Asks about input metadata file
# Shows column detection results
# Reviews sample matches
# Confirms sample type classifications
# Confirms data normalizations
# Generates final metadata with all samples (matched + unmatched)
```

### 8.2 FASTQ-Only Mode (Current Behavior)
```bash
# FASTQ analysis only
poetry run ee-metadata ./fastq_data
# Analyzes FASTQ files
# Shows detected markers
# User selects markers to include
# Generates metadata from FASTQ analysis only
```

### 8.3 Metadata-Only Mode (New)
```bash
# Metadata conversion only (no FASTQ files in directory)
poetry run ee-metadata ./empty_directory --input-metadata ./existing_metadata.csv
# Detects no FASTQ files
# Offers metadata-only mode
# Shows column detection results
# Normalizes dates/coordinates/sample types
# Prompts for project markers
# User selects markers from primers.csv
# Generates standardized metadata with selected markers
```

### 8.4 Mixed Scenarios
**Some samples match, some don't:**
- Matched samples: Full data from both FASTQ and metadata
- Unmatched FASTQ samples: FASTQ data + empty metadata fields
- Unmatched metadata entries: Metadata data + empty FASTQ fields
- Final output contains ALL samples from both sources

## 9. Dependencies

### New Python Packages Needed:
- `python-dateutil` - For flexible date parsing
- `fuzzywuzzy` or `rapidfuzz` - For fuzzy string matching
- `python-Levenshtein` - For string distance calculations (optional, for performance)

### Updates to Existing Code:
- Modify CLI argument parsing
- Extend interactive prompts
- Update metadata generation workflow
- Enhance error handling throughout

## 10. Testing Strategy

### Test Cases:
- Various CSV formats and encodings
- Different column naming conventions
- Edge cases in sample name matching
- Multiple date and coordinate formats
- Various sample type classification scenarios
- Ambiguous control/sample indicators
- Unmatched samples (FASTQ without metadata, metadata without FASTQ)
- Metadata-only mode with manual marker selection
- Mixed scenarios with partial matches
- Large datasets (performance testing)
- Malformed input data

### Test Data:
- Create sample input metadata files with different formats
- Include edge cases and common naming patterns
- Test with real-world data variations