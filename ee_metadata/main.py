import gzip
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import polars as pl
import typer
from dateutil import parser as date_parser
from rapidfuzz import fuzz
from rich.console import Console
from rich.table import Table

# Initialize Typer app and Rich console for nice terminal output
app = typer.Typer()
console = Console()


def clear_terminal():
    """Clear the terminal screen for better UX."""
    os.system("cls" if os.name == "nt" else "clear")


def get_iupac_regex(primer: str) -> str:
    """Converts a primer sequence with IUPAC ambiguity codes into a regular expression."""
    iupac_map = {
        "R": "[AG]",
        "Y": "[CT]",
        "S": "[GC]",
        "W": "[AT]",
        "K": "[GT]",
        "M": "[AC]",
        "B": "[CGT]",
        "D": "[AGT]",
        "H": "[ACT]",
        "V": "[ACG]",
        "N": "[ACGT]",
    }
    return "".join(iupac_map.get(base, base) for base in primer)


def analyze_fastq_file(
    filepath: Path, primers_df: pl.DataFrame, num_records: int
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Analyzes a gzipped fastq file to find matching primers within the first N records.

    Returns hit counts and percentages for each primer in forward/reverse directions.
    """
    # Initialize hit counters for each primer
    primer_hits = {}
    for row in primers_df.iter_rows(named=True):
        primer_id = row["id"]
        primer_hits[primer_id] = {
            "forward_hits": 0,
            "reverse_hits": 0,
            "fwd_primer": row["forwardSequence"],
            "rev_primer": row["reverseSequence"],
        }

    total_sequences = 0

    try:
        with gzip.open(filepath, "rt") as f:
            for i, line in enumerate(f):
                if i >= num_records * 4:
                    break
                if i % 4 == 1:  # Sequence line in FASTQ format
                    sequence = line.strip()
                    total_sequences += 1

                    # Check each primer against this sequence
                    for row in primers_df.iter_rows(named=True):
                        primer_id, fwd_primer, rev_primer = (
                            row["id"],
                            row["forwardSequence"],
                            row["reverseSequence"],
                        )

                        # Check forward primer
                        if fwd_primer and re.search(
                            get_iupac_regex(fwd_primer), sequence
                        ):
                            primer_hits[primer_id]["forward_hits"] += 1

                        # Check reverse primer
                        if rev_primer and re.search(
                            get_iupac_regex(rev_primer), sequence
                        ):
                            primer_hits[primer_id]["reverse_hits"] += 1

    except Exception as e:
        console.print(f"[bold red]Error reading {filepath.name}:[/bold red] {e}")
        total_sequences = 1  # Avoid division by zero

    # Calculate percentages and format results
    results = {"forward": {}, "reverse": {}}

    for primer_id, hits in primer_hits.items():
        # Only include primers that had hits
        fwd_percentage = (
            (hits["forward_hits"] / total_sequences) * 100 if total_sequences > 0 else 0
        )
        rev_percentage = (
            (hits["reverse_hits"] / total_sequences) * 100 if total_sequences > 0 else 0
        )

        if hits["forward_hits"] > 0:
            results["forward"][primer_id] = {
                "id": primer_id,
                "hits": hits["forward_hits"],
                "total_sequences": total_sequences,
                "percentage": fwd_percentage,
                "fwd_primer": hits["fwd_primer"],
                "rev_primer": hits["rev_primer"],
            }

        if hits["reverse_hits"] > 0:
            results["reverse"][primer_id] = {
                "id": primer_id,
                "hits": hits["reverse_hits"],
                "total_sequences": total_sequences,
                "percentage": rev_percentage,
                "fwd_primer": hits["fwd_primer"],
                "rev_primer": hits["rev_primer"],
            }

    return results


def get_base_name(filename: str) -> str:
    """Generates a common base name from a filename for pairing (e.g., 'Sample_A_R1_001.fastq.gz' -> 'Sample_A')."""
    return re.sub(r"(_S\d+)?_R[12](_001)?\.fastq\.gz$", "", filename)


def get_sample_id(filename: str) -> str:
    """Generates a Sample ID from a filename."""
    match = re.match(r"^(.*?)_S\d+", filename)
    return match.group(1) if match else get_base_name(filename).split("_")[0]


def complete_path(incomplete: str):
    """Custom path completion function."""
    import glob
    import os

    # Handle empty input
    if not incomplete:
        incomplete = "./"

    # Expand user home directory
    incomplete = os.path.expanduser(incomplete)

    # Get matching paths
    if os.path.isdir(incomplete):
        matches = glob.glob(os.path.join(incomplete, "*"))
    else:
        matches = glob.glob(incomplete + "*")

    return matches


# ============================================================================
# METADATA PROCESSING FUNCTIONS
# ============================================================================


def load_and_validate_metadata_csv(filepath: Path) -> Optional[pl.DataFrame]:
    """Load and validate input metadata CSV file."""
    try:
        if not filepath.exists():
            console.print(
                f"[bold red]Error:[/bold red] Metadata file not found: {filepath}"
            )
            return None

        # Try to read the CSV with Polars
        df = pl.read_csv(filepath)

        if df.height == 0:
            console.print(
                f"[bold yellow]Warning:[/bold yellow] Metadata file is empty: {filepath}"
            )
            return None

        console.print(
            f"[bold green]✓[/bold green] Loaded metadata CSV: {df.height} rows, {df.width} columns"
        )
        return df

    except Exception as e:
        console.print(f"[bold red]Error reading metadata CSV:[/bold red] {e}")
        return None


def validate_sample_name_column(
    df: pl.DataFrame, column: str, site_column: Optional[str] = None
) -> Tuple[bool, str]:
    """Validate that a sample name column has unique values and is different from site column."""
    if column not in df.columns:
        return False, "Column not found"

    # Check uniqueness
    values = df[column].to_list()
    unique_values = set(values)

    if len(unique_values) != len(values):
        duplicates = len(values) - len(unique_values)
        return (
            False,
            f"Contains {duplicates} duplicate values - sample names must be unique",
        )

    # Check if same as site column
    if site_column and site_column in df.columns:
        site_values = set(df[site_column].to_list())
        sample_values = set(values)

        if site_values == sample_values:
            return (
                False,
                "Same values as Site column - sample names must be different from site names",
            )

        # Check for significant overlap
        overlap = len(site_values.intersection(sample_values))
        if overlap > len(sample_values) * 0.5:  # More than 50% overlap
            return False, f"Too much overlap with Site column ({overlap} shared values)"

    return True, "Valid unique sample names"


def detect_columns(df: pl.DataFrame) -> Dict[str, Optional[str]]:
    """Intelligently detect relevant columns in the metadata CSV."""
    columns = df.columns
    detected = {
        "site": None,
        "sample_name": None,
        "sample_date": None,
        "latitude": None,
        "longitude": None,
        "sample_type": None,
    }

    # Primary detection patterns for each column type
    patterns = {
        "site": ["site", "location", "station", "area", "region"],
        "sample_name": ["sample", "sample_id", "specimen", "id", "name"],
        "sample_date": ["date", "collected", "sampling", "time"],
        "latitude": ["latitude", "lat", "y_coord"],  # Prioritize exact matches
        "longitude": [
            "longitude",
            "lon",
            "long",
            "x_coord",
        ],  # Prioritize exact matches
        "sample_type": [
            "sample_type",
            "type",
            "control",
            "reference",
            "project",
        ],  # Reordered for better matching
    }

    # Secondary fallback keywords for more lenient matching
    fallback_patterns = {
        "site": ["place", "locality", "point", "spot"],
        "sample_name": ["specimen", "code", "identifier", "label"],
        "sample_date": ["when", "timestamp", "datetime", "collected", "taken"],
        "latitude": [
            "north",
            "south",
            "y",
            "lat_dd",
        ],  # Single letters in fallback only
        "longitude": [
            "east",
            "west",
            "x",
            "lon_dd",
            "lng",
        ],  # Single letters in fallback only
        "sample_type": ["category", "kind", "class", "blank", "treatment"],
    }

    for field, keywords in patterns.items():
        best_match = None
        best_score = 0
        best_method = None

        # First, check for exact matches (highest priority)
        for col in columns:
            col_lower = col.lower()
            col_clean = re.sub(
                r"[^\w]", "", col_lower
            )  # Remove special chars for matching

            # Exact match gets highest priority
            if col_lower == field or col_lower in keywords:
                best_match = col
                best_score = 200  # Very high score for exact matches
                best_method = "exact"
                break

        # If no exact match, continue with other methods
        if not best_match:
            for col in columns:
                col_lower = col.lower()
                col_clean = re.sub(
                    r"[^\w]", "", col_lower
                )  # Remove special chars for matching

                # Method 1: Direct substring matching
                for keyword in keywords:
                    if keyword in col_lower:
                        # Score based on how much of the column name the keyword represents
                        score = (len(keyword) / len(col_lower)) * 100

                        # Special handling for coordinates - prefer full words over single letters
                        if field in ["latitude", "longitude"] and len(col_lower) == 1:
                            score = (
                                score * 0.3
                            )  # Heavily penalize single character matches

                        # Special handling for sample_type - heavily penalize single characters
                        if field == "sample_type" and len(col_lower) == 1:
                            score = (
                                score * 0.1
                            )  # Almost eliminate single character matches

                        # Prevent coordinate fields from matching sample_type patterns
                        if field in ["latitude", "longitude"] and any(
                            term in keyword
                            for term in ["type", "control", "reference", "project"]
                        ):
                            score = score * 0.2

                        if score > best_score:
                            best_score = score
                            best_match = col
                            best_method = "direct"

                # Method 2: Fuzzy matching on full column name
                for keyword in keywords:
                    fuzzy_score = fuzz.ratio(keyword, col_lower)
                    if fuzzy_score > 70:
                        # Apply same penalties for single character columns
                        if field in ["latitude", "longitude"] and len(col_lower) == 1:
                            fuzzy_score = fuzzy_score * 0.3
                        if field == "sample_type" and len(col_lower) == 1:
                            fuzzy_score = fuzzy_score * 0.1

                        # Prevent coordinate fields from matching sample_type patterns
                        if field in ["latitude", "longitude"] and any(
                            term in keyword
                            for term in ["type", "control", "reference", "project"]
                        ):
                            fuzzy_score = fuzzy_score * 0.2

                        if fuzzy_score > best_score:
                            best_score = fuzzy_score
                            best_match = col
                            best_method = "fuzzy"

                # Method 3: Token-based matching (check if any keyword appears in the column)
                col_tokens = col_lower.split()
                for keyword in keywords:
                    for token in col_tokens:
                        if keyword in token or token in keyword:
                            # Boost score for token matches
                            score = 80 + (len(keyword) / len(token)) * 10

                            # Apply penalties for problematic matches
                            if (
                                field in ["latitude", "longitude"]
                                and len(col_lower) == 1
                            ):
                                score = score * 0.3
                            if field == "sample_type" and len(col_lower) == 1:
                                score = score * 0.1

                            # Prevent coordinate fields from matching sample_type patterns
                            if field in ["latitude", "longitude"] and any(
                                term in keyword
                                for term in ["type", "control", "reference", "project"]
                            ):
                                score = score * 0.2

                            if score > best_score:
                                best_score = score
                                best_match = col
                                best_method = "token"

        # If no good match found, try fallback patterns with more lenient matching
        if best_score < 50 and field in fallback_patterns:
            for col in columns:
                col_lower = col.lower()

                # Check fallback keywords
                for keyword in fallback_patterns[field]:
                    if keyword in col_lower:
                        score = 60 + (len(keyword) / len(col_lower)) * 20
                        if score > best_score:
                            best_score = score
                            best_match = col
                            best_method = "fallback"

                # For date fields, be extra lenient - look for any date/time indicators
                if field == "sample_date":
                    date_indicators = [
                        "date",
                        "time",
                        "when",
                        "day",
                        "month",
                        "year",
                        "datetime",
                        "timestamp",
                    ]
                    for indicator in date_indicators:
                        if indicator in col_lower:
                            score = 55 + (len(indicator) / len(col_lower)) * 15
                            if score > best_score:
                                best_score = score
                                best_match = col
                                best_method = "date_fallback"

        # Accept matches with reasonable confidence (lowered threshold)
        # Special handling for sample_type to avoid single-character false positives
        if field == "sample_type" and best_match:
            # Reject single character columns unless they have very high confidence
            if len(best_match) == 1 and best_score < 90:
                continue
            # Prefer longer, more descriptive column names for sample type
            if best_score > 45:
                detected[field] = best_match
        elif best_score > 45:
            detected[field] = best_match

    # Special validation for sample_name column
    if detected["sample_name"]:
        is_valid, validation_msg = validate_sample_name_column(
            df, detected["sample_name"], detected["site"]
        )
        if not is_valid:
            # Try to find an alternative unique column for sample names
            console.print(
                f"[bold yellow]Warning:[/bold yellow] Detected sample name column '{detected['sample_name']}' has issues: {validation_msg}"
            )

            # Look for alternative columns that might work as sample names
            alternative_found = False
            for col in columns:
                if col != detected["sample_name"] and col != detected["site"]:
                    is_valid_alt, _ = validate_sample_name_column(
                        df, col, detected["site"]
                    )
                    if is_valid_alt:
                        console.print(
                            f"[bold blue]Info:[/bold blue] Found alternative unique column for sample names: '{col}'"
                        )
                        detected["sample_name"] = col
                        alternative_found = True
                        break

            if not alternative_found:
                console.print(
                    "[bold yellow]Warning:[/bold yellow] No suitable unique sample name column found. Will use FASTQ-derived names if available."
                )
                detected["sample_name"] = None

    return detected


def normalize_sample_name(name: str) -> str:
    """Normalize sample name for matching."""
    if not name:
        return ""

    # Convert to string and lowercase
    normalized = str(name).lower().strip()

    # Remove common FASTQ suffixes
    patterns_to_remove = [
        r"_r[12]$",  # _R1, _R2, _r1, _r2
        r"_l?\d+$",  # _001, _L001, etc.
        r"\.fastq\.gz$",  # .fastq.gz
        r"\.fq\.gz$",  # .fq.gz
        r"\.fastq$",  # .fastq
        r"\.fq$",  # .fq
    ]

    for pattern in patterns_to_remove:
        normalized = re.sub(pattern, "", normalized)

    # Remove extra whitespace and special characters
    normalized = re.sub(r"[^\w\-]", "", normalized)

    return normalized


def identify_string_columns(metadata_df: pl.DataFrame) -> List[str]:
    """Identify string-based columns in the metadata DataFrame (exclude numeric and date-like columns)."""
    string_columns = []

    for col in metadata_df.columns:
        # Get a sample of non-null values from this column
        sample_values = metadata_df[col].drop_nulls().head(10).to_list()

        if not sample_values:
            continue

        # Check if column contains mostly numeric or date-like values
        is_numeric = True
        is_date_like = True

        for val in sample_values:
            val_str = str(val).strip()
            if not val_str:
                continue

            # Check if it's numeric (including coordinates with degrees)
            try:
                # Try basic float conversion
                float(
                    val_str.replace("°", "")
                    .replace("N", "")
                    .replace("S", "")
                    .replace("E", "")
                    .replace("W", "")
                    .strip()
                )
            except:
                is_numeric = False

            # Check if it's date-like
            try:
                from dateutil import parser as date_parser

                date_parser.parse(val_str)
            except:
                is_date_like = False

            # If it's neither numeric nor date-like, it's likely a string column
            if not is_numeric and not is_date_like:
                break

        # Include column if it's not purely numeric or date-like
        if not (is_numeric or is_date_like):
            string_columns.append(col)

    console.print(
        f"[dim]Identified string-based columns for matching: {', '.join(string_columns)}[/dim]"
    )
    return string_columns


def find_comprehensive_sample_matches(
    fastq_samples: List[str], metadata_df: pl.DataFrame
) -> Dict[str, Dict]:
    """Find matches between FASTQ samples and metadata using comprehensive fuzzy search across all string columns."""
    # Identify string-based columns to search
    string_columns = identify_string_columns(metadata_df)

    if not string_columns:
        console.print(
            "[yellow]Warning: No suitable string columns found for matching[/yellow]"
        )
        return {}

    matches = {}

    for fastq_sample in fastq_samples:
        fastq_normalized = normalize_sample_name(fastq_sample)
        best_match_row = None
        best_total_score = 0
        best_match_details = {}

        # Check each row in the metadata
        for row_idx in range(metadata_df.height):
            row_data = metadata_df.row(row_idx, named=True)
            row_total_score = 0
            row_match_details = {}

            # Check each string column in this row
            for col in string_columns:
                col_value = row_data.get(col)
                if col_value is None or str(col_value).strip() == "":
                    continue

                col_normalized = normalize_sample_name(str(col_value))
                if not col_normalized:
                    continue

                # Calculate similarity score for this column
                col_score = 0
                match_type = None

                # Method 1: Exact match
                if fastq_normalized == col_normalized:
                    col_score = 100
                    match_type = "exact"
                # Method 2: Substring match
                elif (
                    fastq_normalized in col_normalized
                    or col_normalized in fastq_normalized
                ):
                    col_score = 85
                    match_type = "substring"
                # Method 3: Fuzzy matching
                else:
                    fuzzy_score = fuzz.ratio(fastq_normalized, col_normalized)
                    if fuzzy_score > 70:  # Only consider good fuzzy matches
                        col_score = fuzzy_score * 0.8  # Slightly penalize fuzzy matches
                        match_type = "fuzzy"

                if col_score > 0:
                    row_match_details[col] = {
                        "score": col_score,
                        "method": match_type,
                        "value": str(col_value),
                        "normalized": col_normalized,
                    }
                    row_total_score += col_score

            # Check if this row is the best match so far
            if row_total_score > best_total_score:
                best_total_score = row_total_score
                best_match_row = row_idx
                best_match_details = row_match_details

        # Only include matches above a minimum threshold
        if best_total_score > 50:  # Require at least a decent match
            matches[fastq_sample] = {
                "metadata_index": best_match_row,
                "total_score": best_total_score,
                "match_details": best_match_details,
                "matched_columns": len(best_match_details),
            }

    return matches


def find_sample_matches(
    fastq_samples: List[str], metadata_df: pl.DataFrame, sample_col: str
) -> Dict[str, Dict]:
    """Find matches between FASTQ samples and metadata samples."""
    if sample_col not in metadata_df.columns:
        return {}

    metadata_samples = metadata_df[sample_col].to_list()
    matches = {}

    for fastq_sample in fastq_samples:
        fastq_normalized = normalize_sample_name(fastq_sample)
        best_match = None
        best_score = 0
        best_method = None

        for i, meta_sample in enumerate(metadata_samples):
            if meta_sample is None:
                continue

            meta_normalized = normalize_sample_name(str(meta_sample))

            # Skip empty samples
            if not fastq_normalized or not meta_normalized:
                continue

            # Method 1: Exact match
            if fastq_normalized == meta_normalized:
                best_match = i
                best_score = 100
                best_method = "exact"
                break

            # Method 2: Substring match
            if (
                fastq_normalized in meta_normalized
                or meta_normalized in fastq_normalized
            ):
                score = 90
                if score > best_score:
                    best_match = i
                    best_score = score
                    best_method = "substring"

            # Method 3: Fuzzy matching
            fuzzy_score = fuzz.ratio(fastq_normalized, meta_normalized)
            if fuzzy_score > best_score and fuzzy_score > 80:
                best_match = i
                best_score = fuzzy_score
                best_method = "fuzzy"

        if best_match is not None:
            matches[fastq_sample] = {
                "metadata_index": best_match,
                "metadata_sample": metadata_samples[best_match],
                "score": best_score,
                "method": best_method,
            }

    return matches


def normalize_date(date_str: str) -> Optional[str]:
    """Normalize date string to YYYY-MM-DD format."""
    if not date_str or date_str in [None, "", "null", "NULL"]:
        return None

    try:
        # Use dateutil parser for flexible date parsing
        parsed_date = date_parser.parse(str(date_str))
        return parsed_date.strftime("%Y-%m-%d")
    except:
        return None


def normalize_coordinate(coord_str: str) -> Optional[float]:
    """Convert coordinate string to decimal degrees."""
    if not coord_str or coord_str in [None, "", "null", "NULL"]:
        return None

    try:
        coord_str = str(coord_str).strip()

        # Handle simple decimal degrees
        if re.match(r"^-?\d+\.?\d*$", coord_str):
            return float(coord_str)

        # Handle coordinates with cardinal directions
        # Example: "119.09929000° W" or "35.35854900° N"
        direction_match = re.search(r"([NSEW])\s*$", coord_str.upper())
        direction = direction_match.group(1) if direction_match else None

        # Remove direction and trailing whitespace for parsing
        if direction:
            coord_str = coord_str[: direction_match.start()].strip()

        # Remove degree symbol if present
        coord_str = coord_str.replace("°", "").strip()

        # Handle DMS format: 45°7'24.4"
        dms_match = re.match(r'(\d+)[°\s]+(\d+)\'([\d.]+)"?', coord_str)
        if dms_match:
            degrees = int(dms_match.group(1))
            minutes = int(dms_match.group(2))
            seconds = float(dms_match.group(3))
            decimal = degrees + minutes / 60 + seconds / 3600
        else:
            # Try simple float conversion after cleaning
            decimal = float(coord_str)

        # Apply direction
        if direction in ["S", "W"]:
            decimal = -decimal

        return decimal

    except Exception as e:
        console.print(
            f"[yellow]Warning: Could not parse coordinate '{coord_str}': {e}[/yellow]"
        )
        return None


def analyze_sample_type_column(df: pl.DataFrame, column: str) -> Dict[str, Any]:
    """Analyze sample type column contents to derive classification rules."""
    if column not in df.columns:
        return {"rule": None, "confidence": 0, "explanation": "Column not found"}

    # Get unique values from the column
    unique_values = df[column].unique().to_list()
    unique_values = [
        str(v).strip() for v in unique_values if v is not None and str(v).strip()
    ]

    if not unique_values:
        return {"rule": None, "confidence": 0, "explanation": "No valid values found"}

    console.print(
        f"\n[bold blue]📊 Analyzing Sample Type Column: '{column}'[/bold blue]"
    )
    console.print(f"Unique values found: {', '.join(unique_values)}")

    # Define pattern groups for analysis
    sample_patterns = [
        ("sample", ["sample", "field sample", "environmental sample", "specimen"]),
        ("project", ["project site", "project", "study site", "field site"]),
        ("environmental", ["environmental", "field"]),
        ("field", ["field"]),  # Field samples are generally environmental samples
    ]

    control_patterns = [
        ("control", ["control", "ctrl", "control site"]),
        ("reference", ["reference", "ref", "reference site"]),
        (
            "blank",
            ["blank", "negative", "field blank", "extraction blank", "pcr blank"],
        ),
    ]

    # Analyze patterns in the values
    sample_matches = []
    control_matches = []

    for value in unique_values:
        value_lower = value.lower()

        # Check sample patterns
        for pattern_name, patterns in sample_patterns:
            for pattern in patterns:
                if pattern in value_lower:
                    sample_matches.append((value, pattern_name, pattern))
                    break

        # Check control patterns
        for pattern_name, patterns in control_patterns:
            for pattern in patterns:
                if pattern in value_lower:
                    control_matches.append((value, pattern_name, pattern))
                    break

    # Determine best classification strategy
    rule_info = None

    if len(sample_matches) > 0 and len(control_matches) > 0:
        # Mixed case - prefer positive matching for the more specific/descriptive terms
        sample_terms = set(match[2] for match in sample_matches)
        control_terms = set(match[2] for match in control_matches)

        # Prefer longer, more specific terms
        if any(len(term) > 7 for term in sample_terms):
            # Use positive sample matching
            best_sample_term = max(sample_terms, key=len)
            rule_info = {
                "rule": best_sample_term,
                "rule_type": "positive",
                "confidence": 85,
                "explanation": f"Detected mixed types. Using positive match for '{best_sample_term}' to identify samples.",
                "sample_matches": sample_matches,
                "control_matches": control_matches,
            }
        elif any(len(term) > 7 for term in control_terms):
            # Use negative control matching
            best_control_term = max(control_terms, key=len)
            rule_info = {
                "rule": f"!{best_control_term}",
                "rule_type": "negative",
                "confidence": 85,
                "explanation": f"Detected mixed types. Using negative match for '{best_control_term}' to identify samples (anything NOT containing this term).",
                "sample_matches": sample_matches,
                "control_matches": control_matches,
            }
        elif len(sample_matches) >= len(control_matches):
            best_sample_term = max(sample_terms, key=len)
            rule_info = {
                "rule": best_sample_term,
                "rule_type": "positive",
                "confidence": 70,
                "explanation": f"Mixed types detected. Using positive match for '{best_sample_term}' (appears in {len(sample_matches)} values).",
                "sample_matches": sample_matches,
                "control_matches": control_matches,
            }
        else:
            best_control_term = max(control_terms, key=len)
            rule_info = {
                "rule": f"!{best_control_term}",
                "rule_type": "negative",
                "confidence": 70,
                "explanation": f"Mixed types detected. Using negative match for '{best_control_term}' (appears in {len(control_matches)} values).",
                "sample_matches": sample_matches,
                "control_matches": control_matches,
            }

    elif len(sample_matches) > 0:
        # Only sample patterns found - check if we need a broader pattern
        sample_terms = set(match[2] for match in sample_matches)
        matched_values = set(match[0] for match in sample_matches)

        # If not all values are matched, we need a more inclusive approach
        if len(matched_values) < len(unique_values):
            # Look for a pattern that appears in most values
            unmatched_values = set(unique_values) - matched_values
            console.print(
                f"[yellow]Note: Some values not matched by sample patterns: {', '.join(unmatched_values)}[/yellow]"
            )

            # Check if unmatched values contain sample-like terms
            all_sample_like = True
            for unmatched in unmatched_values:
                unmatched_lower = unmatched.lower()
                # Check if it contains any typical control terms
                contains_control_terms = any(
                    term in unmatched_lower
                    for term in ["control", "reference", "blank", "negative"]
                )
                if contains_control_terms:
                    all_sample_like = False
                    break

            if all_sample_like:
                # All unmatched values also seem to be samples, so default to all being samples
                rule_info = {
                    "rule": "sample",  # Use a generic positive rule
                    "rule_type": "positive",
                    "confidence": 75,
                    "explanation": "All values appear to be sample types. Using generic 'sample' rule to identify them.",
                    "sample_matches": sample_matches,
                    "control_matches": control_matches,
                }
            else:
                # Use the best detected sample term
                best_sample_term = max(sample_terms, key=len)
                rule_info = {
                    "rule": best_sample_term,
                    "rule_type": "positive",
                    "confidence": 80,
                    "explanation": f"Mixed sample types detected. Using positive match for '{best_sample_term}' (partial coverage).",
                    "sample_matches": sample_matches,
                    "control_matches": control_matches,
                }
        else:
            # All values matched - find a pattern that covers all values
            # Group matches by value to see coverage
            value_coverage = {}
            for value, pattern_name, pattern in sample_matches:
                if value not in value_coverage:
                    value_coverage[value] = []
                value_coverage[value].append(pattern)

            # Find a pattern that appears in all values
            common_patterns = None
            for value, patterns in value_coverage.items():
                if common_patterns is None:
                    common_patterns = set(patterns)
                else:
                    common_patterns = common_patterns.intersection(set(patterns))

            if common_patterns:
                # Use the longest common pattern
                best_common_term = max(common_patterns, key=len)
                rule_info = {
                    "rule": best_common_term,
                    "rule_type": "positive",
                    "confidence": 95,
                    "explanation": f"Only sample indicators found. Using common pattern '{best_common_term}' that appears in all values.",
                    "sample_matches": sample_matches,
                    "control_matches": control_matches,
                }
            else:
                # No common pattern - use the longest term (fallback)
                best_sample_term = max(sample_terms, key=len)
                rule_info = {
                    "rule": best_sample_term,
                    "rule_type": "positive",
                    "confidence": 85,
                    "explanation": f"Only sample indicators found. Using most specific pattern '{best_sample_term}' (partial coverage).",
                    "sample_matches": sample_matches,
                    "control_matches": control_matches,
                }

    elif len(control_matches) > 0:
        # Only control patterns found - use negative matching
        control_terms = set(match[2] for match in control_matches)
        best_control_term = max(control_terms, key=len)
        rule_info = {
            "rule": f"!{best_control_term}",
            "rule_type": "negative",
            "confidence": 90,
            "explanation": f"Only control indicators found. Using negative match for '{best_control_term}' (anything NOT containing this term will be considered a sample).",
            "sample_matches": sample_matches,
            "control_matches": control_matches,
        }

    else:
        # No clear patterns - suggest manual classification
        rule_info = {
            "rule": None,
            "rule_type": "manual",
            "confidence": 0,
            "explanation": "No clear sample/control patterns detected. Manual classification recommended.",
            "sample_matches": sample_matches,
            "control_matches": control_matches,
        }

    return rule_info


def get_user_confirmed_sample_type_rule(rule_info: Dict[str, Any]) -> Optional[str]:
    """Get user confirmation or override for sample type classification rule."""
    if not rule_info or rule_info["confidence"] == 0:
        console.print(
            "[bold yellow]⚠️  No automatic rule could be derived.[/bold yellow]"
        )
        console.print("Please specify a manual rule for sample type classification.")
        console.print(
            "[dim]Examples: 'sample', 'project', '!control', '!reference'[/dim]"
        )

        while True:
            manual_rule = typer.prompt(
                "Enter classification rule (or 'skip' to use default logic)"
            ).strip()
            if manual_rule.lower() == "skip":
                return None
            if manual_rule:
                return manual_rule
            console.print("[bold red]Please enter a valid rule or 'skip'[/bold red]")

    # Display the suggested rule
    console.print("\n[bold green]📋 Suggested Classification Rule:[/bold green]")
    console.print(f"Rule: [cyan]{rule_info['rule']}[/cyan]")
    console.print(f"Confidence: [yellow]{rule_info['confidence']}%[/yellow]")
    console.print(f"Logic: {rule_info['explanation']}")

    # Show examples of how the rule would classify values
    if rule_info["sample_matches"] or rule_info["control_matches"]:
        console.print("\n[bold blue]Classification Preview:[/bold blue]")

        # Show sample matches
        if rule_info["sample_matches"]:
            console.print("[green]Would be classified as SAMPLES:[/green]")
            for value, pattern_name, pattern in rule_info["sample_matches"]:
                console.print(f"  • '{value}' (contains '{pattern}')")

        # Show control matches
        if rule_info["control_matches"]:
            console.print("[red]Would be classified as CONTROLS:[/red]")
            for value, pattern_name, pattern in rule_info["control_matches"]:
                console.print(f"  • '{value}' (contains '{pattern}')")

    console.print("\n[bold blue]💡 Rule Usage:[/bold blue]")
    if rule_info["rule_type"] == "positive":
        console.print(
            f"Positive match: Values containing '{rule_info['rule']}' = Sample, others = Control"
        )
    elif rule_info["rule_type"] == "negative":
        clean_rule = rule_info["rule"][1:]  # Remove the '!' prefix
        console.print(
            f"Negative match: Values NOT containing '{clean_rule}' = Sample, values containing it = Control"
        )

    # Get user confirmation
    if typer.confirm(f"\nUse this rule: '{rule_info['rule']}'?", default=True):
        return rule_info["rule"]

    # Allow manual override
    console.print("\n[bold blue]Manual Rule Entry:[/bold blue]")
    console.print("[dim]Examples:[/dim]")
    console.print("[dim]  'sample' - positive match (contains 'sample' = Sample)[/dim]")
    console.print(
        "[dim]  'project' - positive match (contains 'project' = Sample)[/dim]"
    )
    console.print(
        "[dim]  '!control' - negative match (NOT containing 'control' = Sample)[/dim]"
    )
    console.print(
        "[dim]  '!reference' - negative match (NOT containing 'reference' = Sample)[/dim]"
    )

    while True:
        manual_rule = typer.prompt(
            "Enter your custom rule (or 'skip' for default logic)"
        ).strip()
        if manual_rule.lower() == "skip":
            return None
        if manual_rule:
            return manual_rule
        console.print("[bold red]Please enter a valid rule or 'skip'[/bold red]")


def classify_sample_type_with_rule(type_str: str, rule: Optional[str] = None) -> bool:
    """Classify sample type as Sample (True) or Control (False) using a rule."""
    if not type_str:
        return True  # Default to Sample

    type_lower = str(type_str).lower()

    # If no rule provided, use default logic
    if not rule:
        return classify_sample_type_default(type_str)

    # Parse rule
    if rule.startswith("!"):
        # Negative rule - if type_str does NOT contain the pattern, it's a sample
        pattern = rule[1:].lower()
        return pattern not in type_lower
    else:
        # Positive rule - if type_str contains the pattern, it's a sample
        pattern = rule.lower()
        return pattern in type_lower


def classify_sample_type_default(type_str: str) -> bool:
    """Default classification logic (original function)."""
    if not type_str:
        return True  # Default to Sample

    type_lower = str(type_str).lower()

    # Control indicators
    control_terms = [
        "control",
        "ctrl",
        "reference",
        "ref",
        "blank",
        "negative",
        "project site",
        "control site",
        "reference site",
        "field blank",
        "extraction blank",
        "pcr blank",
    ]

    # Check for control indicators
    for term in control_terms:
        if term in type_lower:
            return False

    # Check for single letter codes
    if type_lower in ["c", "r", "ctrl", "ref", "neg"]:
        return False

    # Default to Sample
    return True


# Keep original function name for backward compatibility
def classify_sample_type(type_str: str) -> bool:
    """Classify sample type as Sample (True) or Control (False)."""
    return classify_sample_type_default(type_str)


@app.command()
def generate(
    input_dir: Path = typer.Argument(
        None,
        help="Path to the directory with fastq.gz files.",
        autocompletion=complete_path,
    ),
    primers: Path = typer.Option(
        Path(__file__).parent / "primers.csv",
        "--primers",
        "-p",
        help="Path to the primers CSV file.",
        autocompletion=complete_path,
    ),
    input_metadata: Path = typer.Option(
        None,
        "--input-metadata",
        "-m",
        help="Path to input metadata CSV file to merge with FASTQ analysis.",
        autocompletion=complete_path,
    ),
    output: Path = typer.Option(
        "metadata.csv",
        "--output",
        "-o",
        help="Name of the output metadata CSV file.",
        autocompletion=complete_path,
    ),
    num_records: int = typer.Option(
        100,
        "--num-records",
        "-n",
        help="Number of records to check in each fastq file.",
    ),
    force_pairing: bool = typer.Option(
        False,
        "--force-pairing",
        help="Force pairing based on filenames if primer analysis is inconclusive.",
    ),
):
    """
    Generates a metadata CSV by analyzing FASTQ files for primer sequences and pairing reads.
    Supports three modes:
    1. FASTQ analysis only (current behavior)
    2. FASTQ analysis + metadata integration
    3. Metadata-only mode (conversion without FASTQ files)
    """
    # Display ASCII splash screen
    clear_terminal()
    splash_art = r"""
╔══════════════════════════════════════════════════════════════╗
║  ______ _____  _   _          __  __      _            _     ║
║ |  ____|  __ \| \ | |   /\   |  \/  |    | |          | |    ║
║ | |__  | |  | |  \| |  /  \  | \  / | ___| |_ __ _  __| |_   ║
║ |  __| | |  | | . ` | / /\ \ | |\/| |/ _ \ __/ _` |/ _` | | | ║
║ | |____| |__| | |\  |/ ____ \| |  | |  __/ || (_| | (_| | |_| ║
║ |______|_____/|_| \_/_/    \_\_|  |_|\___|\__\__,_|\__,_|\__,_|║
║                                                              ║
║         🧬 Analyze FASTQ files & generate metadata 🧬        ║
╚══════════════════════════════════════════════════════════════╝
    """

    console.print(f"[bold cyan]{splash_art}[/bold cyan]")
    console.print("[bold green]Starting Metadata Generation...[/bold green]\n")

    # Interactive prompts for missing required inputs
    if input_dir is None:
        console.print(
            "\n[bold blue]💡 Tip:[/bold blue] For tab completion, use: [cyan]poetry run ee-metadata <TAB>[/cyan] to autocomplete paths"
        )
        while True:
            input_path = typer.prompt(
                "\nEnter the path to the directory containing .fastq.gz files"
            )
            input_dir = Path(input_path).expanduser()  # Handle ~ for home directory
            if input_dir.exists() and input_dir.is_dir():
                break
            console.print(
                f"[bold red]Error:[/bold red] Directory '{input_path}' does not exist or is not a directory."
            )

    # Check if primers file exists, prompt if not
    if not primers.exists():
        console.print(
            f"[bold yellow]Warning:[/bold yellow] Default primers file not found at {primers}"
        )
        console.print(
            "[bold blue]💡 Tip:[/bold blue] Use [cyan]--primers <TAB>[/cyan] for path autocompletion"
        )
        while True:
            primer_path = typer.prompt("Enter the path to the primers CSV file")
            primers = Path(primer_path).expanduser()
            if primers.exists() and primers.is_file():
                break
            console.print(
                f"[bold red]Error:[/bold red] File '{primer_path}' does not exist or is not a file."
            )

    # Prompt for output file if user wants to change it
    if (
        typer.confirm(f"\nUse default output filename '{output}'?", default=True)
        is False
    ):
        output_path = typer.prompt("Enter the output filename")
        output = Path(output_path).expanduser()

    # Handle input metadata processing
    metadata_df = None
    column_mapping = {}

    # Prompt for input metadata if not provided
    if input_metadata is None:
        if typer.confirm(
            "\nDo you have input metadata to merge with the FASTQ analysis?",
            default=False,
        ):
            while True:
                metadata_path = typer.prompt(
                    "Enter the path to the input metadata CSV file"
                )
                input_metadata = Path(metadata_path).expanduser()
                if input_metadata.exists() and input_metadata.is_file():
                    break
                console.print(
                    f"[bold red]Error:[/bold red] File '{metadata_path}' does not exist or is not a file."
                )

    # Load and process metadata if provided
    sample_type_rule = None
    if input_metadata is not None:
        clear_terminal()
        metadata_df = load_and_validate_metadata_csv(input_metadata)
        if metadata_df is not None:
            console.print(
                "\n[bold blue]🔍 Detecting columns in metadata...[/bold blue]"
            )
            detected_columns = detect_columns(metadata_df)

            # Display detected columns with enhanced information
            table = Table(title="Detected Metadata Columns")
            table.add_column("Field", style="cyan")
            table.add_column("Detected Column", style="green")
            table.add_column("Status", style="yellow")

            for field, col in detected_columns.items():
                if col:
                    # Validate sample name column
                    if field == "sample_name":
                        is_valid, validation_msg = validate_sample_name_column(
                            metadata_df, col, detected_columns.get("site")
                        )
                        if is_valid:
                            status = "✓ Valid & unique"
                        else:
                            status = f"⚠️ {validation_msg}"
                    else:
                        status = "✓ Detected"
                else:
                    status = "✗ Not found"
                table.add_row(field.replace("_", " ").title(), col or "None", status)

            console.print(table)

            # Analyze sample type column if detected
            if detected_columns.get("sample_type"):
                sample_type_col = detected_columns["sample_type"]
                rule_info = analyze_sample_type_column(metadata_df, sample_type_col)
                sample_type_rule = get_user_confirmed_sample_type_rule(rule_info)

            # Interactive column mapping confirmation
            if typer.confirm("\nReview and confirm column mappings?", default=True):
                for field, detected_col in detected_columns.items():
                    field_display = field.replace("_", " ").title()
                    if detected_col:
                        if typer.confirm(
                            f"Use '{detected_col}' for {field_display}?", default=True
                        ):
                            column_mapping[field] = detected_col
                        else:
                            # Manual selection
                            console.print(
                                f"Available columns: {', '.join(metadata_df.columns)}"
                            )
                            manual_col = typer.prompt(
                                f"Enter column name for {field_display} (or 'skip')"
                            )
                            if (
                                manual_col.lower() != "skip"
                                and manual_col in metadata_df.columns
                            ):
                                column_mapping[field] = manual_col
                    elif field == "sample_name":
                        console.print(
                            f"Available columns: {', '.join(metadata_df.columns)}"
                        )
                        console.print(
                            "[bold blue]Note:[/bold blue] Sample names must be unique. If you skip, we'll derive names from FASTQ files."
                        )
                        manual_col = typer.prompt(
                            f"Enter column name for {field_display} (or 'skip')"
                        )
                        if (
                            manual_col.lower() != "skip"
                            and manual_col in metadata_df.columns
                        ):
                            # Validate the manually selected column
                            is_valid, validation_msg = validate_sample_name_column(
                                metadata_df, manual_col, column_mapping.get("site")
                            )
                            if is_valid:
                                column_mapping[field] = manual_col
                            else:
                                console.print(
                                    f"[bold red]Warning:[/bold red] Column '{manual_col}' is not suitable: {validation_msg}"
                                )
                                console.print(
                                    "[bold yellow]Skipping sample name - will derive from FASTQ files.[/bold yellow]"
                                )
                    else:
                        console.print(
                            f"Available columns: {', '.join(metadata_df.columns)}"
                        )
                        manual_col = typer.prompt(
                            f"Enter column name for {field_display} (or 'skip')"
                        )
                        if (
                            manual_col.lower() != "skip"
                            and manual_col in metadata_df.columns
                        ):
                            column_mapping[field] = manual_col
            else:
                # Use auto-detected columns
                column_mapping = {
                    k: v for k, v in detected_columns.items() if v is not None
                }

    # 1. Load Primers using Polars
    try:
        primers_df = pl.read_csv(primers)
        console.print(
            f":white_check_mark: Loaded {len(primers_df)} primers from [cyan]{primers.name}[/cyan] using Polars"
        )
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] Could not load primer file: {e}")
        raise typer.Exit(code=1)

    # 2. Find and Analyze Fastq Files
    fastq_files = list(input_dir.glob("*.fastq.gz"))
    metadata_only_mode = False

    if not fastq_files:
        if metadata_df is not None:
            console.print(
                f"[bold yellow]No FASTQ files found in {input_dir}[/bold yellow]"
            )
            if typer.confirm(
                "Would you like to proceed in metadata-only mode?", default=True
            ):
                metadata_only_mode = True
                console.print(
                    "[bold blue]Proceeding in metadata-only mode...[/bold blue]"
                )
            else:
                console.print(
                    "[bold red]Error:[/bold red] No FASTQ files to process and metadata-only mode declined."
                )
                raise typer.Exit(code=1)
        else:
            console.print(
                f"[bold red]Error:[/bold red] No fastq.gz files found in {input_dir}"
            )
            raise typer.Exit(code=1)
    else:
        console.print(f"Found {len(fastq_files)} fastq.gz files. Analyzing...")

    # Handle FASTQ analysis or metadata-only mode
    paired_samples = []

    if not metadata_only_mode:
        # Standard FASTQ analysis
        file_analysis = {}
        with typer.progressbar(fastq_files, label="Analyzing files") as progress:
            for f in progress:
                file_analysis[f.name] = analyze_fastq_file(f, primers_df, num_records)

        # 3. Classify and Pair Files
        forward_files, reverse_files = {}, {}
        for name, analysis in file_analysis.items():
            # Check if we have any forward or reverse hits (analysis is now a dict of dicts)
            has_fwd = bool(analysis["forward"])
            has_rev = bool(analysis["reverse"])

            if has_fwd and not has_rev:
                forward_files[name] = analysis["forward"]
            elif has_rev and not has_fwd:
                reverse_files[name] = analysis["reverse"]
            elif force_pairing or "_R1" in name:
                forward_files[name] = analysis.get("forward", {})
            elif force_pairing or "_R2" in name:
                reverse_files[name] = analysis.get("reverse", {})

        console.print(
            f"Classified [bold yellow]{len(forward_files)}[/bold yellow] forward and [bold yellow]{len(reverse_files)}[/bold yellow] reverse files."
        )

        # Pair FASTQ samples
        unmatched_fwd = list(forward_files.keys())

        for fwd_file, fwd_markers in forward_files.items():
            base_name = get_base_name(fwd_file)
            rev_match = next(
                (
                    r_file
                    for r_file in reverse_files
                    if get_base_name(r_file) == base_name
                ),
                None,
            )

            if rev_match:
                rev_markers = reverse_files[rev_match]

                # Combine markers from both files, summing percentages for same primers
                all_markers = {}

                # Add forward markers
                for primer_id, marker_data in fwd_markers.items():
                    all_markers[primer_id] = {
                        "id": primer_id,
                        "fwd_primer": marker_data["fwd_primer"],
                        "rev_primer": marker_data["rev_primer"],
                        "total_percentage": marker_data["percentage"],
                        "forward_hits": marker_data["hits"],
                        "reverse_hits": 0,
                        "forward_percentage": marker_data["percentage"],
                        "reverse_percentage": 0,
                    }

                # Add reverse markers, combining with forward if present
                for primer_id, marker_data in rev_markers.items():
                    if primer_id in all_markers:
                        all_markers[primer_id]["reverse_hits"] = marker_data["hits"]
                        all_markers[primer_id]["reverse_percentage"] = marker_data[
                            "percentage"
                        ]
                        all_markers[primer_id]["total_percentage"] += marker_data[
                            "percentage"
                        ]
                    else:
                        all_markers[primer_id] = {
                            "id": primer_id,
                            "fwd_primer": marker_data["fwd_primer"],
                            "rev_primer": marker_data["rev_primer"],
                            "total_percentage": marker_data["percentage"],
                            "forward_hits": 0,
                            "reverse_hits": marker_data["hits"],
                            "forward_percentage": 0,
                            "reverse_percentage": marker_data["percentage"],
                        }

                paired_samples.append(
                    {
                        "Sample ID": get_sample_id(fwd_file),
                        "Fastq Forward Reads Filename": fwd_file,
                        "Fastq Reverse Reads Filename": rev_match,
                        "markers": list(all_markers.values()),
                    }
                )
                unmatched_fwd.remove(fwd_file)
                del reverse_files[rev_match]

        console.print(
            f"Successfully paired [bold green]{len(paired_samples)}[/bold green] samples."
        )
    else:
        # Metadata-only mode - create samples from metadata
        console.print(
            "[bold blue]Creating samples from metadata entries...[/bold blue]"
        )
        if "sample_name" in column_mapping:
            sample_col = column_mapping["sample_name"]
            for i in range(metadata_df.height):
                row_data = metadata_df.row(i, named=True)
                sample_name = row_data.get(sample_col, f"Sample_{i+1}")
                paired_samples.append(
                    {
                        "Sample ID": str(sample_name),
                        "Fastq Forward Reads Filename": "",
                        "Fastq Reverse Reads Filename": "",
                        "markers": [],  # Will be populated with user-selected markers
                    }
                )
        console.print(
            f"Created [bold green]{len(paired_samples)}[/bold green] samples from metadata."
        )

    # 4. Marker Selection (FASTQ Detection or Manual Selection)
    clear_terminal()
    console.print("[bold green]📊 Analyzing Marker Detection Results...[/bold green]")
    confirmed_markers = []

    if not metadata_only_mode and paired_samples:
        # Standard FASTQ mode - detect markers from analysis using percentage scores
        marker_prevalence_scores = {}
        marker_sample_counts = {}
        total_samples = len(paired_samples)

        for sample in paired_samples:
            for marker in sample["markers"]:
                marker_id = marker["id"]
                total_percentage = marker.get("total_percentage", 0)

                # Sum the total percentages across all samples
                if marker_id not in marker_prevalence_scores:
                    marker_prevalence_scores[marker_id] = 0
                    marker_sample_counts[marker_id] = 0

                marker_prevalence_scores[marker_id] += total_percentage
                marker_sample_counts[marker_id] += 1

        if marker_prevalence_scores:
            # Sort markers by total prevalence score (most to least)
            sorted_markers = sorted(
                marker_prevalence_scores.items(), key=lambda x: x[1], reverse=True
            )

            # Separate markers into high and low priority based on average hit rate
            high_priority_threshold = 15.0  # 15% average hit rate threshold
            high_priority_markers = []
            low_priority_markers = []

            for marker_id, total_score in sorted_markers:
                sample_count = marker_sample_counts[marker_id]
                avg_hit_rate = total_score / sample_count if sample_count > 0 else 0

                if avg_hit_rate >= high_priority_threshold:
                    high_priority_markers.append((marker_id, total_score))
                else:
                    low_priority_markers.append((marker_id, total_score))

            # Display high priority markers
            console.print(
                f"\n[bold green]🎯 High Priority Markers (≥{high_priority_threshold}% avg hit rate)[/bold green]"
            )
            if high_priority_markers:
                console.print(
                    "┌────────────────────────────────────┬─────────┬──────────────┬────────────────┐"
                )
                console.print(
                    "│ Marker ID                          │ Samples │ Avg Hit Rate │ Total Score    │"
                )
                console.print(
                    "├────────────────────────────────────┼─────────┼──────────────┼────────────────┤"
                )

                for marker_id, total_score in high_priority_markers:
                    sample_count = marker_sample_counts[marker_id]
                    avg_hit_rate = total_score / sample_count if sample_count > 0 else 0
                    console.print(
                        f"│ {marker_id:<34} │ {sample_count:^7} │ {avg_hit_rate:^12.1f}% │ {total_score:^14.1f} │"
                    )

                console.print(
                    "└────────────────────────────────────┴─────────┴──────────────┴────────────────┘"
                )
            else:
                console.print("[dim]No high priority markers detected[/dim]")

            # Display low priority markers
            console.print(
                f"\n[bold yellow]⚠️  Low Priority Markers (<{high_priority_threshold}% avg hit rate)[/bold yellow]"
            )
            if low_priority_markers:
                console.print(
                    "┌────────────────────────────────────┬─────────┬──────────────┬────────────────┐"
                )
                console.print(
                    "│ Marker ID                          │ Samples │ Avg Hit Rate │ Total Score    │"
                )
                console.print(
                    "├────────────────────────────────────┼─────────┼──────────────┼────────────────┤"
                )

                for marker_id, total_score in low_priority_markers:
                    sample_count = marker_sample_counts[marker_id]
                    avg_hit_rate = total_score / sample_count if sample_count > 0 else 0
                    console.print(
                        f"│ {marker_id:<34} │ {sample_count:^7} │ {avg_hit_rate:^12.1f}% │ {total_score:^14.1f} │"
                    )

                console.print(
                    "└────────────────────────────────────┴─────────┴──────────────┴────────────────┘"
                )
            else:
                console.print("[dim]No low priority markers detected[/dim]")

            console.print(
                "\n[dim]Total Score = Sum of hit percentages across all samples[/dim]"
            )
            console.print(
                "[dim]Avg Hit Rate = Average percentage of sequences matching primer in samples where found[/dim]"
            )
            console.print(
                f"[dim]Priority Threshold = {high_priority_threshold}% average hit rate[/dim]"
            )

            # Check for disambiguation needed among high priority markers
            disambiguated_high_priority = []
            if high_priority_markers:
                # Group high priority markers by their "marker" column value
                marker_groups = {}
                for marker_id, total_score in high_priority_markers:
                    # Look up the marker value from primers_df
                    marker_value = None
                    for row in primers_df.iter_rows(named=True):
                        if row["id"] == marker_id:
                            marker_value = row.get("marker", marker_id)
                            break

                    if marker_value not in marker_groups:
                        marker_groups[marker_value] = []
                    marker_groups[marker_value].append((marker_id, total_score))

                # Check for groups with multiple markers
                needs_disambiguation = []
                for marker_value, markers in marker_groups.items():
                    if len(markers) > 1:
                        needs_disambiguation.append((marker_value, markers))

                # Handle disambiguation
                if needs_disambiguation:
                    clear_terminal()
                    console.print(
                        "\n[bold blue]🔍 Marker Disambiguation Required[/bold blue]"
                    )
                    console.print(
                        "Multiple high priority markers detected for the same target gene/region:"
                    )

                    for marker_value, markers in needs_disambiguation:
                        console.print(
                            f"\n[bold yellow]Target: {marker_value}[/bold yellow]"
                        )
                        for i, (marker_id, total_score) in enumerate(markers, 1):
                            sample_count = marker_sample_counts[marker_id]
                            avg_hit_rate = (
                                total_score / sample_count if sample_count > 0 else 0
                            )
                            console.print(
                                f"  {i}. {marker_id} (avg hit rate: {avg_hit_rate:.1f}%)"
                            )

                        console.print(
                            "[dim]Options: Enter marker number, 'both' to include all, or 'skip' to exclude[/dim]"
                        )

                        while True:
                            choice = (
                                typer.prompt(f"Select marker(s) for {marker_value}")
                                .strip()
                                .lower()
                            )

                            if choice == "skip":
                                break
                            elif choice == "both":
                                disambiguated_high_priority.extend(markers)
                                break
                            else:
                                try:
                                    choice_num = int(choice)
                                    if 1 <= choice_num <= len(markers):
                                        disambiguated_high_priority.append(
                                            markers[choice_num - 1]
                                        )
                                        break
                                    else:
                                        console.print(
                                            f"[bold red]Error:[/bold red] Please enter a number between 1 and {len(markers)}"
                                        )
                                except ValueError:
                                    console.print(
                                        "[bold red]Error:[/bold red] Please enter a number, 'both', or 'skip'"
                                    )

                # Add non-conflicting markers directly
                for marker_value, markers in marker_groups.items():
                    if len(markers) == 1:
                        disambiguated_high_priority.extend(markers)

                # Update high_priority_markers with disambiguated results
                high_priority_markers = disambiguated_high_priority

            # Interactive marker selection
            console.print(
                "\n[bold yellow]💡 Recommendation: Focus on high priority markers for reliable results.[/bold yellow]"
            )

            if typer.confirm(
                "\nWould you like to review and select which markers to include?",
                default=True,
            ):
                clear_terminal()
                console.print("[bold green]🎯 Final Marker Selection[/bold green]")
                console.print(
                    "\n[bold green]Select markers to include in the final metadata:[/bold green]"
                )
                console.print(
                    "[dim]Type marker numbers separated by commas (e.g., 1,2,4), 'high' for high priority only, or 'all' for all markers[/dim]"
                )

                # Display numbered list for selection organized by priority
                marker_counter = 1
                console.print("\n[bold green]🎯 High Priority Markers:[/bold green]")
                high_priority_indices = {}
                for marker_id, total_score in high_priority_markers:
                    sample_count = marker_sample_counts[marker_id]
                    sample_percentage = (sample_count / total_samples) * 100
                    avg_hit_rate = total_score / sample_count if sample_count > 0 else 0
                    console.print(
                        f"  {marker_counter}. {marker_id} ({sample_count}/{total_samples} samples, {sample_percentage:.1f}%, avg hit rate: {avg_hit_rate:.1f}%)"
                    )
                    high_priority_indices[marker_counter] = marker_id
                    marker_counter += 1

                console.print("\n[bold yellow]⚠️  Low Priority Markers:[/bold yellow]")
                low_priority_indices = {}
                for marker_id, total_score in low_priority_markers:
                    sample_count = marker_sample_counts[marker_id]
                    sample_percentage = (sample_count / total_samples) * 100
                    avg_hit_rate = total_score / sample_count if sample_count > 0 else 0
                    console.print(
                        f"  {marker_counter}. {marker_id} ({sample_count}/{total_samples} samples, {sample_percentage:.1f}%, avg hit rate: {avg_hit_rate:.1f}%)"
                    )
                    low_priority_indices[marker_counter] = marker_id
                    marker_counter += 1

                # Combine indices for easier lookup
                all_indices = {**high_priority_indices, **low_priority_indices}
                total_markers = len(all_indices)

                while True:
                    selection = typer.prompt("\nEnter your selection").strip().lower()

                    if selection == "all":
                        confirmed_markers = [
                            marker_id for marker_id, _ in sorted_markers
                        ]
                        break
                    elif selection == "high":
                        confirmed_markers = [
                            marker_id for marker_id, _ in high_priority_markers
                        ]
                        break

                    try:
                        # Parse comma-separated numbers
                        indices = [int(x.strip()) for x in selection.split(",")]
                        if all(1 <= i <= total_markers for i in indices):
                            confirmed_markers = [all_indices[i] for i in indices]
                            break
                        else:
                            console.print(
                                f"[bold red]Error:[/bold red] Please enter numbers between 1 and {total_markers}"
                            )
                    except ValueError:
                        console.print(
                            "[bold red]Error:[/bold red] Please enter numbers separated by commas, 'high', or 'all'"
                        )

                console.print(
                    f"\n[bold green]Selected markers:[/bold green] {', '.join(confirmed_markers)}"
                )
            else:
                # Use all detected markers if user skips selection
                confirmed_markers = [marker_id for marker_id, _ in sorted_markers]
                console.print(
                    f"[bold blue]Using all detected markers:[/bold blue] {', '.join(confirmed_markers)}"
                )
        else:
            console.print(
                "[bold yellow]No markers detected in FASTQ analysis.[/bold yellow]"
            )
    else:
        # Metadata-only mode - manual marker selection from primers.csv
        console.print("\n[bold blue]🧬 Manual Marker Selection[/bold blue]")
        console.print("Select markers from available primers for your project:")

        # Get all available primers
        all_primers = []
        for row in primers_df.iter_rows(named=True):
            all_primers.append((row["id"], row.get("name", ""), row.get("marker", "")))

        # Display available primers
        console.print("\nAvailable primers:")
        for i, (primer_id, name, marker) in enumerate(all_primers, 1):
            display_name = f"{name} ({marker})" if name and marker else marker or ""
            console.print(f"  {i}. {primer_id} {display_name}")

        console.print(
            "[dim]Type marker numbers separated by commas (e.g., 1,2,4) or 'all' for all markers[/dim]"
        )

        while True:
            selection = typer.prompt("\nEnter your selection").strip().lower()

            if selection == "all":
                confirmed_markers = [primer_id for primer_id, _, _ in all_primers]
                break

            try:
                # Parse comma-separated numbers
                indices = [int(x.strip()) for x in selection.split(",")]
                if all(1 <= i <= len(all_primers) for i in indices):
                    confirmed_markers = [all_primers[i - 1][0] for i in indices]
                    break
                else:
                    console.print(
                        f"[bold red]Error:[/bold red] Please enter numbers between 1 and {len(all_primers)}"
                    )
            except ValueError:
                console.print(
                    "[bold red]Error:[/bold red] Please enter numbers separated by commas or 'all'"
                )

        console.print(
            f"\n[bold green]Selected markers:[/bold green] {', '.join(confirmed_markers)}"
        )

        # Update paired_samples with selected markers
        for sample in paired_samples:
            sample["markers"] = [{"id": marker_id} for marker_id in confirmed_markers]

    # 5. Create Global Consistent Marker Mapping
    if not metadata_only_mode and "sorted_markers" in locals():
        # Use only user-confirmed primer IDs in sorted order (maintaining original detection frequency order)
        global_primer_ids = [
            marker_id
            for marker_id, _ in sorted_markers
            if marker_id in confirmed_markers
        ]
    else:
        # In metadata-only mode, use confirmed markers in the order they were selected
        global_primer_ids = confirmed_markers

    console.print(
        f"Using confirmed primer IDs: [cyan]{', '.join(global_primer_ids)}[/cyan]"
    )

    # Create primer ID position mapping (e.g., 12S-V5 -> Marker 1, 16S_515F_926R -> Marker 2, etc.)
    primer_position_map = {
        primer_id: i + 1 for i, primer_id in enumerate(global_primer_ids)
    }

    # Create a lookup of primer info from the primers DataFrame for consistent data
    primer_info_lookup = {}
    for row in primers_df.iter_rows(named=True):
        primer_info_lookup[row["id"]] = {
            "id": row["id"],
            "fwd_primer": row["forwardSequence"],
            "rev_primer": row["reverseSequence"],
        }

    # Build metadata columns dynamically based on detected primer IDs
    base_cols = [
        "Data type",
        "Site",
        "Sample ID",
        "Sample Type",
        "Latitude",
        "Longitude",
        "Spatial Uncertainty",
        "Sample Date",
        "Sequencing Platform",
        "Sequence Length",
        "Adapter Type",
    ]

    # Add marker columns for all detected primer IDs
    marker_cols = []
    for i, primer_id in enumerate(global_primer_ids, 1):
        marker_cols.extend(
            [f"Marker {i}", f"Marker {i} ForwardPS", f"Marker {i} ReversePS"]
        )

    # Add unmapped columns from input metadata as additional user-defined columns
    additional_cols = []
    if metadata_df is not None:
        # Get all mapped column names from column_mapping
        mapped_columns = set(column_mapping.values())

        # Find unmapped columns (excluding empty/null values)
        unmapped_columns = []
        for col in metadata_df.columns:
            if col not in mapped_columns:
                unmapped_columns.append(col)

        # Add unmapped columns as additional metadata columns
        additional_cols = unmapped_columns
        if additional_cols:
            console.print(
                f"[bold blue]📋 Adding {len(additional_cols)} unmapped metadata columns as additional fields:[/bold blue]"
            )
            console.print(f"   {', '.join(additional_cols)}")

    # Add remaining template columns
    end_cols = [
        "Fastq Forward Reads Filename",
        "Fastq Reverse Reads Filename",
        "Substrate",
        "Depth (m)",
        "Environmental Feature",
        "Environmental Setting",
    ]

    # Build final column list: base + markers + additional user columns + template end columns
    metadata_cols = base_cols + marker_cols + end_cols + additional_cols

    # 6. Sample Matching and Metadata Integration
    sample_matches = {}
    unmatched_metadata_indices = set(
        range(metadata_df.height if metadata_df is not None else 0)
    )

    if metadata_df is not None:
        # Find matches between FASTQ samples and metadata using comprehensive search
        fastq_sample_names = [sample["Sample ID"] for sample in paired_samples]
        sample_matches = find_comprehensive_sample_matches(
            fastq_sample_names, metadata_df
        )

        # Track which metadata rows were matched
        for match_info in sample_matches.values():
            unmatched_metadata_indices.discard(match_info["metadata_index"])

        console.print("\n[bold blue]📋 Sample Matching Results[/bold blue]")
        console.print(f"Matched samples: [green]{len(sample_matches)}[/green]")
        console.print(
            f"Unmatched FASTQ samples: [yellow]{len(paired_samples) - len(sample_matches)}[/yellow]"
        )
        console.print(
            f"Unmatched metadata entries: [yellow]{len(unmatched_metadata_indices)}[/yellow]"
        )

        if sample_matches:
            console.print("\n[bold green]🎯 Match Details:[/bold green]")
            for fastq_sample, match_info in list(sample_matches.items())[
                :5
            ]:  # Show first 5 matches
                console.print(
                    f"  • {fastq_sample} → Row {match_info['metadata_index']}"
                )
                console.print(
                    f"    Total Score: {match_info['total_score']:.1f}, Columns Matched: {match_info['matched_columns']}"
                )
                for col, details in match_info["match_details"].items():
                    console.print(
                        f"    - {col}: '{details['value']}' ({details['method']}, {details['score']:.1f})"
                    )

            if len(sample_matches) > 5:
                console.print(f"    ... and {len(sample_matches) - 5} more matches")

    # 7. Generate Metadata Table with Confirmed Markers for ALL Samples
    rows = []

    # Process FASTQ samples (matched and unmatched)
    for sample in paired_samples:
        row = {col: "" for col in metadata_cols}

        # Start with FASTQ-derived data
        row.update(
            {
                "Sample ID": sample["Sample ID"],
                "Fastq Forward Reads Filename": sample["Fastq Forward Reads Filename"],
                "Fastq Reverse Reads Filename": sample["Fastq Reverse Reads Filename"],
            }
        )

        # Add metadata if sample is matched
        if sample["Sample ID"] in sample_matches and metadata_df is not None:
            match_info = sample_matches[sample["Sample ID"]]
            metadata_row = metadata_df.row(match_info["metadata_index"], named=True)

            # Map metadata fields to output columns
            if column_mapping.get("site"):
                row["Site"] = metadata_row.get(column_mapping["site"], "")

            if column_mapping.get("sample_date"):
                raw_date = metadata_row.get(column_mapping["sample_date"], "")
                row["Sample Date"] = normalize_date(str(raw_date)) if raw_date else ""

            if column_mapping.get("latitude"):
                raw_lat = metadata_row.get(column_mapping["latitude"], "")
                row["Latitude"] = normalize_coordinate(str(raw_lat)) if raw_lat else ""

            if column_mapping.get("longitude"):
                raw_lon = metadata_row.get(column_mapping["longitude"], "")
                row["Longitude"] = normalize_coordinate(str(raw_lon)) if raw_lon else ""

            if column_mapping.get("sample_type"):
                raw_type = metadata_row.get(column_mapping["sample_type"], "")
                row["Sample Type"] = (
                    "Sample"
                    if classify_sample_type_with_rule(str(raw_type), sample_type_rule)
                    else "Control"
                )

        # Fill in marker data
        for primer_id, position in primer_position_map.items():
            if primer_id in primer_info_lookup:
                primer_info = primer_info_lookup[primer_id]

                # Always populate the marker data (even if not detected in this sample)
                row[f"Marker {position}"] = primer_info["id"]
                row[f"Marker {position} ForwardPS"] = primer_info["fwd_primer"] or ""
                row[f"Marker {position} ReversePS"] = primer_info["rev_primer"] or ""

        # Fill in additional metadata columns if sample is matched
        if sample["Sample ID"] in sample_matches and metadata_df is not None:
            match_info = sample_matches[sample["Sample ID"]]
            metadata_row = metadata_df.row(match_info["metadata_index"], named=True)

            # Add all unmapped columns to preserve original metadata
            for col in additional_cols:
                if col in metadata_row:
                    row[col] = metadata_row[col] or ""

        rows.append(row)

    # Add unmatched metadata entries as additional rows
    if metadata_df is not None and unmatched_metadata_indices:
        console.print(
            f"\n[bold blue]Adding {len(unmatched_metadata_indices)} unmatched metadata entries...[/bold blue]"
        )

        for metadata_index in unmatched_metadata_indices:
            row = {col: "" for col in metadata_cols}
            metadata_row = metadata_df.row(metadata_index, named=True)

            # Fill in metadata fields
            if column_mapping.get("sample_name"):
                row["Sample ID"] = str(
                    metadata_row.get(
                        column_mapping["sample_name"], f"MetaOnly_{metadata_index}"
                    )
                )
            else:
                row["Sample ID"] = f"MetaOnly_{metadata_index}"

            if column_mapping.get("site"):
                row["Site"] = metadata_row.get(column_mapping["site"], "")

            if column_mapping.get("sample_date"):
                raw_date = metadata_row.get(column_mapping["sample_date"], "")
                row["Sample Date"] = normalize_date(str(raw_date)) if raw_date else ""

            if column_mapping.get("latitude"):
                raw_lat = metadata_row.get(column_mapping["latitude"], "")
                row["Latitude"] = normalize_coordinate(str(raw_lat)) if raw_lat else ""

            if column_mapping.get("longitude"):
                raw_lon = metadata_row.get(column_mapping["longitude"], "")
                row["Longitude"] = normalize_coordinate(str(raw_lon)) if raw_lon else ""

            if column_mapping.get("sample_type"):
                raw_type = metadata_row.get(column_mapping["sample_type"], "")
                row["Sample Type"] = (
                    "Sample"
                    if classify_sample_type_with_rule(str(raw_type), sample_type_rule)
                    else "Control"
                )

            # Fill in marker data (same as FASTQ samples)
            for primer_id, position in primer_position_map.items():
                if primer_id in primer_info_lookup:
                    primer_info = primer_info_lookup[primer_id]
                    row[f"Marker {position}"] = primer_info["id"]
                    row[f"Marker {position} ForwardPS"] = (
                        primer_info["fwd_primer"] or ""
                    )
                    row[f"Marker {position} ReversePS"] = (
                        primer_info["rev_primer"] or ""
                    )

            # Add all unmapped columns to preserve original metadata
            for col in additional_cols:
                if col in metadata_row:
                    row[col] = metadata_row[col] or ""

            # Leave FASTQ filename columns empty for metadata-only entries
            row["Fastq Forward Reads Filename"] = ""
            row["Fastq Reverse Reads Filename"] = ""

            rows.append(row)

    # 8. Save Output using Polars
    if rows:
        # Create a Polars DataFrame and write to CSV
        df_out = pl.DataFrame(rows, schema=metadata_cols)
        df_out.write_csv(output)

        # Summary report
        clear_terminal()
        total_samples = len(rows)
        fastq_samples = len(paired_samples)
        metadata_only_samples = total_samples - fastq_samples

        console.print("\n🎉 [bold green]Metadata Generation Complete![/bold green]")
        console.print(
            "\n:tada: [bold green]Metadata file generated successfully![/bold green]"
        )
        console.print(f"📄 Output file: [bold cyan]{output}[/bold cyan]")
        console.print(f"📊 Total samples: [bold yellow]{total_samples}[/bold yellow]")
        if metadata_df is not None:
            console.print(f"   • FASTQ samples: [green]{fastq_samples}[/green]")
            console.print(
                f"   • Metadata-only samples: [blue]{metadata_only_samples}[/blue]"
            )
            console.print(f"   • Matched samples: [green]{len(sample_matches)}[/green]")
        console.print(
            f"🧬 Markers included: [cyan]{len(global_primer_ids)}[/cyan] ({', '.join(global_primer_ids)})"
        )

        if metadata_only_mode:
            console.print("🔧 [bold blue]Mode:[/bold blue] Metadata-only conversion")
        elif metadata_df is not None:
            console.print(
                "🔧 [bold blue]Mode:[/bold blue] FASTQ analysis + metadata integration"
            )
        else:
            console.print("🔧 [bold blue]Mode:[/bold blue] FASTQ analysis only")
    else:
        console.print(
            "[bold yellow]Warning: No samples were found to generate metadata.[/bold yellow]"
        )


if __name__ == "__main__":
    app()
