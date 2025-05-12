# Process raw DOR data into compressed research-grade dataset and create visualization stores 


## Environment Setup

To set up the environment for the Ocean CDR Atlas, follow these steps:
1. **Clone the Repository**: 
   ```bash
   git clone
   ```
2. **Set Up the Environment**: 
   ```bash
    conda env create -f environment.yml
    ```

3. **Verify Your Setup**:
    ```bash
    python dor_cli.py setup-environment
    ```

Prerequisites
Access to a Linux system (preferably an HPC environment like NERSC's Perlmutter)
Basic knowledge of Python and command-line interfaces
Familiarity with scientific data formats (NetCDF, Zarr)
Environment Setup
Clone the repository:

Set up the environment:

Verify your setup:

Your First Data Processing Task
Let's process a single case to understand the basic workflow:

List available cases:

Process a specific case:

Check the output directory for your processed data:

How-To Guides
How to Process a Single Case
To process a specific case manually:

Options:

--output-dir or -o: Specify output directory
--data-dir or -d: Specify input data directory
--workers or -w: Number of worker processes
--use-dask: Use Dask for parallel processing
--verbose or -v: Show detailed output
Example:

How to Process Multiple Cases
To process all available cases or a filtered subset:

Options:

--output-dir or -o: Specify output directory
--data-dir or -d: Specify input data directory
--polygon or -p: Filter by polygon ID
--max-cases: Maximum number of cases to process
--workers or -w: Number of worker processes
--use-dask: Use Dask for parallel processing
--verbose or -v: Show detailed output
Example:

How to Create Visualization Pyramids
To build visualization pyramids for CDR Atlas data:

Options:

--polygon-ids or -p: Specific polygon IDs to process
--polygon-range or -pr: Range of polygon IDs (start, end)
--injection-months or -im: Injection months to process
--input-dir or -i: Input directory with processed NetCDF files
--levels or -l: Number of pyramid levels to generate
Example:

How to Submit Batch Jobs on HPC
For processing large datasets on an HPC system:

Edit the template job script:

Submit jobs for multiple polygons with the job submission script:

Options:

--start or -s: Start polygon ID
--end or -e: End polygon ID
--wait or -w: Seconds to wait between checks
Monitor your jobs:

Explanations
CDR Atlas Architecture
The Ocean CDR Atlas consists of several components:

Data Processing Pipeline: Processes raw model output into research-grade data
Visualization System: Creates multi-resolution pyramids for web visualization
Job Management: Scripts for running on HPC systems
Configuration System: Settings for directories and data organization
Data Processing Flow
The system uses a polygon-based approach, where each "polygon" represents a geographic region where CDR interventions are simulated. The data is organized by polygon ID and injection date.

Data Model
Data is organized as follows:

By polygon ID: Geographic regions (0-689)
By injection date: When the CDR intervention begins (e.g., 1999-01, 1999-04)
By elapsed time: Time since the intervention
With counterfactual comparison: Data includes both the intervention scenario and a counterfactual (what would have happened without intervention)
Visualization Pyramid Design
The visualization system uses a pyramidal approach:

Data is regridded to different resolution levels
Each level represents a zoom level for visualization
Data is stored in Zarr format for efficient cloud access
Both experimental (with CDR) and delta (difference from counterfactual) bands are included
This design allows for efficient visualization in web browsers, with appropriate resolutions loading as users zoom in and out.

Reference
CLI Command Reference
list-cases
List all available cases with their metadata.

process-case
Process a single case, compressing and saving all associated files.

process-all
Process all available cases or a filtered subset.

build-vis-pyramid
Build visualization pyramids for CDR Atlas data.

create-template-vis-pyramid
Create a template for the visualization store.

setup-environment
Setup and validate the processing environment.

Key Functions
process_single_case_with_dask: Process a single case using Dask
process_single_case_no_dask: Process a single case using ProcessPoolExecutor
open_compress_and_save_file: Open, compress, and save a single file
process_and_create_pyramid: Process data and create visualization pyramid
reduction: Apply data reduction operations to dataset
Configuration Options
Configuration is managed through the data_config.py file:

get_scratch_dir(): Temporary storage directory
get_dask_log_dir(): Dask log directory
get_dask_local_dir(): Dask local directory
get_compressed_data_dir(): Output directory for compressed data
get_data_archive_dir(): Input directory for raw data
Variable Definitions
Variables are categorized in variables.py by priority and type:

PRIORITY_1_VARS: Highest priority variables to include
PRIORITY_2_VARS: Medium priority variables
COUNTERFACTUAL_VARS: Variables related to the counterfactual scenario
COORDS: Coordinate variables
VARS_TO_DROP: Variables that can be dropped to save space
Data Processing Options
When using use_dask=True (the option in your query), the system uses Dask for parallel processing, which is particularly useful when:

Processing large datasets that exceed memory capacity
Working with distributed computing resources
Requiring scalability across multiple nodes
Dask processing is enabled with the --use-dask flag in the CLI commands and relies on a scheduler for task distribution.


Joblib Caching in Ocean CDR Atlas
How-To Guide: Using the Caching System
The Ocean CDR Atlas implements a powerful caching mechanism using joblib.Memory that can significantly improve performance by avoiding redundant computations. Here's how to leverage this feature effectively:

Understanding the Caching System
The caching system works by:

Storing the inputs and outputs of expensive function calls
Returning cached results when the same function is called with identical inputs
Persisting cache across sessions in the joblib_cache_dir
How to Use Cached Functions
No special syntax is needed to use cached functions - the system automatically retrieves cached results when available:

Managing the Cache
To clear the cache when needed:

Explanation: Caching Architecture
How Caching Works
The setup_memory function initializes a persistent cache in the specified directory:

When a function is wrapped with memory.cache(), joblib:

Computes a hash of the function and its arguments
Checks if results for this hash exist in the cache
Returns cached results if available
Otherwise, executes the function and stores results for future use
Cache Performance Benefits
The caching system provides several advantages:

Time Savings: Avoid repeating expensive data processing operations
Resource Efficiency: Reduce computational load on the cluster
Iterative Development: Quickly test changes without reprocessing unchanged data
Fault Tolerance: Resume interrupted processing without starting over
Cached Functions in Ocean CDR Atlas
Key functions benefiting from caching include:

process_single_case_with_dask: Dask-based processing of individual cases
process_single_case_no_dask: Thread-based processing of individual cases
open_compress_and_save_file: File compression and transformation operations
Reference: Caching Configuration
Cache Directory Structure
The cache is stored in a directory structure within $SCRATCH/dor/joblib_cache:

Cache Size Considerations
Monitor cache size with: du -sh $SCRATCH/dor/joblib_cache
For large-scale processing, cache can grow to many gigabytes
Consider clearing cache periodically on shared systems
Cache files exempt from automatic SCRATCH purging policies
Best Practices
Function Purity: Ensure cached functions are deterministic (same inputs → same outputs)
Selective Caching: Cache compute-intensive functions, not I/O-bound ones
Version Awareness: Clear cache when code changes affect function behavior
Parameter Stability: Use stable, reproducible function parameters for better cache hit rates
For more details on joblib caching, see the joblib documentation.


## zarr v2 vs v3 

- store 2b requires using zarr v2 because of the string encoding issue introduced by zarr v3

## Usage Examples

you can use the CLI tool in several ways:

```bash
# Setup and check the environment
python dor_cli.py setup-environment

# Research-grade data processing
python dor_cli.py research list-cases
python dor_cli.py research process-case my-case-name
python dor_cli.py research process-all --polygon 123

# Visualization pyramid generation
python dor_cli.py vis build-vis-pyramid output.zarr
python dor_cli.py vis create-template-vis-pyramid template.zarr
```

