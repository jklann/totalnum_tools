# totalnum_tools
Tools that assist with analytics on i2b2 patient count data.

The i2b2 patient counting scripts are part of the [main i2b2 data repository](https://github.com/i2b2/i2b2-data]). The latest released version are documented in the [i2b2 release notes](https://community.i2b2.org/wiki/display/RM/1.7.12a+Release+Notes#id-1.7.12aReleaseNotes-TotalnumScriptsSetuptotalnum), though the documentation in GitHub has not yet been updated for the major improvement that is in GitHub.

To install the i2b2 scripts:

* Load the [stored procedures](https://github.com/i2b2/i2b2-data/tree/master/edu.harvard.i2b2.data/Release_1-7/NewInstall/Metadata/scripts/procedures)
* Add the [totalnum and totalnum_report tables to i2b2](https://github.com/i2b2/i2b2-data/tree/master/edu.harvard.i2b2.data/Release_1-7/Upgrade/Metadata/scripts) 

To use these tools:

* Export each i2b2's totalnum_report as a CSV file.
* Export the ACT ontology as a single CSV file per the instructions in totalnum_builddb
* Execute totalnum_builddb_v2.py to build the SQLite database.
* Run totalnum_dashboard_v2.py to explore the SQLite database.
