# Step 2: Submit Jobs

## Overview

Distributed-CellProfiler works by breaking your analysis into a series of smaller jobs based on the metadata and groupings you've specified in your pipeline.
The choice of how to group your images is largely dependent on the details of your experiment.
For example, pipelines to create [illumination correction functions](http://onlinelibrary.wiley.com/doi/10.1111/jmi.12178/full) are usually run on a per-plate basis, while pipelines with a large number of memory intensive measurement steps such as [CellPainting](http://www.nature.com/nprot/journal/v11/n9/full/nprot.2016.105.html) are grouped based on plate, well, and site so that each node only analyzes one site at a time.  

Once you've decided on a grouping, you're ready to start configuring your job file.
Once your job file is configured, simply use `python run.py submitJob files/{YourJobFile}.json` to send all the jobs to the SQS queue [specified in your config file](step_1_configuration.md).

***

**Distributed-CellProfiler only works for pipelines with extracted metadata and specified groupings and which use the LoadData OR on h5 containers created by the CreateBatchFiles module**, though we hope to add support for file-lists in the future.
See [this page](https://github.com/CellProfiler/CellProfiler/wiki/Adapting-CellProfiler-to-a-LIMS-environment) for more information about running CellProfiler headless with file-lists versus LoadData CSVs.  

**The grouping specified in your pipeline MUST match what you specify here in order to successfully run jobs.**

Due to CellProfiler's image-loading mechanisms, experiments with >10,000 image sites can begin to suffer from decreased performance.
Breaking such experiments down into a number of smaller CSVs may increase your processing throughput.  
If using LoadData, make sure your "Base image location" is set to "None".

***

## Configuring your job file

* **pipeline:** The path to your pipeline file.
* **data_file:** The path to your LoadData.csv, batch file, or file list file.
* **input:** The path to your default input directory.
This is not necessary for every pipeline but can be helpful when non-image files are needed in the pipeline (such as a text file containing quality control rules for the FlagImage module or a metadata file for use with file lists).
DO NOT set this to a large directory, or CellProfiler will try to scan the entire thing before running your pipeline.
* **output:** The top output directory you'd like your files placed in.
* **output_structure:** By default, Distributed-CellProfiler will put your output in subfolders created by hyphenating all your Metadata entries (see below) in order (e.g. if the individual group being processed was `{"Metadata": "Metadata_Plate=Plate1,Metadata_Well=A01"}`, the output would be placed in `output_top_directory/Plate1-A01`.)
If you'd like a different folder structure, you may designate one here (e.g. if you set `"output_structure": "Metadata_Plate/Metadata_Well"` then the previous example would output to `output_top_directory/Plate1/A01`.
This setting is optional.
Job files that don't include it will use the default structure.
* **groups:** The list of all the groups of images you'd like to process.
For large numbers of groups, it may be helpful to create this list separately as a .txt file you can then append into the job's JSON file.
You may create this yourself in your favorite scripting language.
Alternatively, you can use the following additional tools to help you create and format this list:
  * `batches.sh` allows you to provide a list of all the individual metadata components (plates, columns, rows, etc).
    It then uses [GNU parallel](https://www.gnu.org/software/parallel/parallel_tutorial.html) to create a formatted text file with all the possible combinations of the components you provided.
    This approach is best when you have a large number of groups and the group structure is uniform.

      Example: for a 96-well plate experiment where one there are 3 plates and the experiment is grouped by Plate and Well, `batches.sh` would read:
        `parallel echo '{\"Metadata\": \"Metadata_Plate={1},Metadata_Well={2}{3}\"},' ::: Plate1 Plate2 Plate3 ::: A B C D E F G H ::: 01 02 03 04 05 06 07 08 09 10 11 12 | sort > batches.txt`
  * You may also use the list of groupings created by calling `cellprofiler --print-groups` from the command line (see [here](https://github.com/CellProfiler/CellProfiler/wiki/Adapting-CellProfiler-to-a-LIMS-environment#cmd) and [here](https://github.com/CellProfiler/Distributed-CellProfiler/issues/52) for more information).  
  Note that for job files that specify groupings in this way, the `output_structure` variable is NOT optional - it must be specified or an error will be returned.

## Alternate job submission: run_batch_general.py

We also support an alternate second path besides `submitJobs` to create the list of jobs - the `run_batch_general.py` file.
This file essentially serves as a "shortcut" to run many common types of stereotyped experiments we run in our lab.
Essentially, if your data follows a regular structure (such as N rows, N columns, N grouping, a particular structure for output, etc.), you may find it useful to take and modify this file for your own usage.  
We recommend new users use the `submitJobs` pathway, as it will help users understand the kinds of information Distributed-CellProfiler needs in order to run properly, but once they are comfortable with it they may find `run_batch_general.py` helps them create jobs faster in the future.

As of Distributed-CellProfiler 2.2.0, `run_batch_general.py` has been reformatted as a CLI tool with greatly enhanced customizeability.
`run_batch_general.py` must be passed 5 pieces of information:

### Required inputs

* `step` is the step that you would like to make jobs for.
Supported steps are `zproj`, `illum`, `qc`, `qc_persite`, `assaydev`, `assaydev_persite`, and `analysis`
* `identifier` is the project identifier (e.g. "cpg0000-jump-pilot" or "2024_11_07_Collaborator_Cell_Painting")
* `batch` is the name of the data batch (e.g. "2020_11_04_CPJUMP1")
* `platelist` is the list of plates to process.
Format the list in quotes with individual plates separated by commas and no spaces (e.g. "Plate1,Plate2,Plate3")

A minimal `run_batch_general.py` command may look like:
"""bash
run_batch_general.py analysis 2024_05_16_Segmentation_Project 2024_10_10_Batch1 "Plate1,Plate2,Plate3"
"""

### Required input for Cell Painting Gallery

Runs being made off of the Cell Painting Gallery require two additional flags:

* `--source <value>` to specify the identifier-specific source of the data.
* `--path-style cpg` is to set the input and output paths as data is structured in the Cell Painting Gallery.
All paths can be overwritten with flags (see below).

A minimal `run_batch_general.py` command for a dataset on the Cell Painting Gallery may look like:
"""bash
run_batch_general.py analysis cpg0000-jump-pilot 2020_11_04_CPJUMP1 "BR00116991,BR00116992" --path-style cpg --source broad
"""

### Plate layout flags

* `--plate-format <value>`: if used, can be `96` or `384` and will overwrite `rows` and `columns` to produce standard 96- or 384-well plate well names (e.g. A01, A02, etc.)
* `--rows <value>`: a custom list of row labels.
Will be combined with `columns` to generate well names.
Separate values with commas and no spaces and surround with quotation marks (e.g. `"A,B,C,D,E,F,G"`)
* `--columns <value>`: a custom list of column labels.
Will be combined with `rows` to generate well names.
Separate values with commas and no spaces and surround with quotation marks (e.g. `"1,2,3,4,5,6,7,8,9,10"`)
* `--wells <value>`: a custom list of wells.
Overwrites `rows` and `columns`.
Separate values with commas and no spaces and surround with quotation marks (e.g. `"C02,D04,E04,N12"`)
* `--no-well-digit-pad`: Formats wells without well digit padding.
Formats wells passed with `--plate format` or `--rows` and `--columns` but not `--wells`.
(e.g. `A1` NOT `A01`)
* `--sites <value>`: a custom list of sites (fields of view) to be analyzed.
Default is 9 sites (1 to 9).
Not used by `illum`, `qc`, or `assaydev` steps.
Separate values with commas and no spaces and surround with quotation marks (e.g. `"1,2,3,4,5,6"`)

### Overwrite structural defaults

* `--output-structure <value>`: overwrite default output structure
* `--output-path <value>`: overwrite default output path
* `--input-path <value>`: overwrite the default path to input files

### Overwrite defaults (for runs using load data .csv's and .cppipe)

* `--pipeline <value>`: overwrite the default pipeline name
* `--pipeline-path <value>`: overwrite the default path to pipelines
* `--datafile-name <value>`: overwrite the default load data .csv name
* `--datafile-path <value>`: overwrite the default path to load data files

### Overwrite defaults (for runs using .h5 batch files)

* `--use-batch`: use h5 batch files instead of load data csv and .cppipe files
* `--batchfile-name <value>`: overwrite default batchfile name
* `--batchfile-path <value>`: overwrite default path to the batchfile
