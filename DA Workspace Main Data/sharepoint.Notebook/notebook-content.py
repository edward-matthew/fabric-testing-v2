# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import sharepy 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sharepy 

s = sharepy.connect("*MySite*.sharepoint.com", "*username*", "*password*") 

r = s.post("https://*MySite*.sharepoint.com/sites/*TeamSiteName*/_api/web/GetFolderByServerRelativeUrl('/sites/*TeamSiteName*/Shared Documents/*FolderName*')/Files/" + "add(overwrite=true,url='test.csv')", "testing,foo,bar")

print r

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
