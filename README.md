# totalnum_tools
Tools that assist with analytics on i2b2 patient count data.

## About the Patient Counting Scripts

The i2b2 patient counting scripts are part of the [main i2b2 data repository](https://github.com/i2b2/i2b2-data). The latest released version are documented in the [i2b2 release notes](https://community.i2b2.org/wiki/display/RM/1.7.12a+Release+Notes#id-1.7.12aReleaseNotes-TotalnumScriptsSetuptotalnum), though the documentation in GitHub has not yet been updated for the major improvement that is in GitHub. The [latest release notes](https://community.i2b2.org/wiki/display/RM/Latest+Release+Notes) will be published with i2b2 1.7.13 at the end of 2021.

## To install the scripts:

* Load the [stored procedures](https://github.com/i2b2/i2b2-data/tree/master/edu.harvard.i2b2.data/Release_1-7/NewInstall/Metadata/scripts/procedures)
* Add the [totalnum and totalnum_report tables to i2b2](https://github.com/i2b2/i2b2-data/tree/master/edu.harvard.i2b2.data/Release_1-7/Upgrade/Metadata/scripts) 

## To participate in the ACT Data Quality Project (for ACT sites):

1. *Run the Scripts*. The scripts will count the total number of patients for every item in your ontology, and fill these in the c_totalnum column of your ontology tables. This can be used for internal quality checks and will display in the i2b2 query tool (not SHRINE), to help researchers pick ontology items that have data behind them.
2. *Export the obfuscated totalnum_report table as a CSV file*. The obfuscated reports of sites’ totalnums can be collected, aggregated, and analyzed by quality checking / exploring tools. (This obfuscation is essentially the same as sharing SHRINE counts; it is obfuscated the same way.) 
3. *Consent to being part of a public file of network-wide averages.* In ACT, we are writing a manuscript about #2, in which we will distribute “network-wide” averages (averages of the percent of patients at each participating site). If you would like to participate, you will be a named author on the paper. 

## The scripts presently require:
1. All data mapping must be in the ontology tables, not (exclusively) the concept_dimension or Adapter Mappings files. 
2. Multiple fact tables cannot coexist in the same hierarchy. (i.e. any single ontology can reference a fact table other than observation_fact, but it cannot reference multiple fact tables)

## Notes on aggregating and analyzing reports (for network admins):
* Export each i2b2's totalnum_report as a CSV file.
* Export the ACT ontology as a single CSV file per the instructions in totalnum_builddb
* Execute totalnum_builddb_v2.py to build the SQLite database.
* Run totalnum_dashboard_v2.py to explore the SQLite database.
* A totalnum_report script of static reports is forthcoming.
