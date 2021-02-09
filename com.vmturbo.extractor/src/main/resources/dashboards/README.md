The folders underneath the "dashboards" folder will be created in Grafana at extractor startup.

Inside each folder there can be a "permissions.yaml" file which will specify permission overrides
for the folder (see
[the Grafana docs](https://grafana.com/docs/grafana/latest/http_api/folder_permissions/)).

The other .yaml files inside each folder are assumed to be Grafana dashboards, and will be created
or updated in Grafana under the appropriate folder.

Rules for Dashboards:

1) **Do Not** change the UID for dashboards! This is what gets used to detect duplicates. This also
   gets used for saved links, so changing the UID will break any bookmarks the customers have to
   specific dashboards.

2) Do not include references to specific datasources. Leave as `null` to use the default data
   source.

3) For "templating", delete the "current" object, and the "options" (if they are dynamically
   generated - e.g. a list of possible groups/clusters).

**Before** (in JSON model):

```
"templating": {
    "list": [
      {
        "current": {
          "selected": false,
          "text": "test-timescale",
          "value": "test-timescale"
        },
        "hide": 0,
        "includeAll": false,
        "label": null,
        "multi": false,
        "name": "DB",
        "options": [],
        "query": "postgres",
        "refresh": 1,
        "regex": "",
        "skipUrlSync": false,
        "type": "datasource"
      }
    ...
```

**After**:

```
"templating": {
    "list": [
      {
        "hide": 0,
        "includeAll": false,
        "label": null,
        "multi": false,
        "name": "DB",
        "options": [],
        "query": "postgres",
        "refresh": 1,
        "regex": "",
        "skipUrlSync": false,
        "type": "datasource"
      }
    ...
```

4) Remove the `id` and `version` fields - they will be unique per installation. You can leave the
   `uid` field.

5) Remove any explicit `time` fields - they are unique to each installation. e.g.:

```
"time": {
"from": "2020-01-01T06:00:00.000Z",
"to": "2020-01-01T06:59:00.000Z"
},
```

6) If you export your finished work from Grafana, it will be in the form of a JSON file. Before you
   check in your work, you should convert it to YAML and remove the JSON file. The utility class
   `ConvertJsonToYaml` in this module should be used to do this; it includes
   transformationsa nd that are important for optimum conversion that are unlikely to be done by any of
   the many online conversion tools you may find. If you run the class from Intellij and set the run
   configuration to use the extractor module's classpath, you should be able to run it without
   command line args. Otherwise, specify the full path to the extractor module's resources source
   directory as the single command line arg.
   
   A step-by-step sequence might look like this:
   1. Export the dashboard to a `.json` file in the dashboard folder within your git workspace.
   2. If you're editing an existing dashboard, make sure the file name is identical to that of the 
      existing `.yaml` file (except the extension, of course).
   3. Run the conversion utility. It should convert the new `.json` file and overwrite the existing
      `.yaml` file. 
   4. Use Intellij's compare utility to compare the old and new `.yaml` files side-by-side 
      (_Git -> Show Diff_).
   5. Use the comparison tool to remove unwanted changes, like `current` blocks etc, as outlined
      above in this document.
   6. Remove the `.json` file so it doesn't get checked in.
      
      
