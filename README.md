birt-kairosdb-datasource-plugin
==================================
[![Gitter](https://badges.gitter.im/Join Chat.svg)](https://gitter.im/Kratos-ISE/birt-kairosdb-datasource-plugin?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

This is a plugin that allows BIRT to support KairosDB as datasource.

# Install

- Download and install the <i>BIRT Designer All-in-one</i> release at https://www.eclipse.org/downloads/packages/eclipse-ide-java-and-report-developers/lunar
- Download the release zip with the jar files in the release section of the Github project (you may also build the source)
- Add the two jar files in the release archive into the <b>dropins</b> folder of your BIRT installation.
- Start BIRT eclipse with a version of java 7 (the query builder will not work with other versions of java). To do so add
```
-vm
C:\Program Files\Java\jre7\bin\server\jvm.dll
```
to the eclipse.ini file in your BIRT installation. Your path might be different.

# Create a simple data set

It works quite the same as with other BIRT datasources.
- Create a new report with `File>New>Other>Business Intelligence and Reporting Tool>Report`
- In the `Data Explorer` tab right click on `Data Sources` to create a Kairosdb Data Source
- When your data source has been created, right click on `Data Sets` to create a new data set with a query to KairosDB
- Once you have chosen the name of your data set and clicked on Next, the KairosDB Web UI should appear. You can build your query and generate it by clicking on `Graph` or `Generate query`.
- You can either validate your dataset or use the other tab to parameterize your query.
- You can now use this data in all your BIRT tables and charts!

# Parameterize your time range

You might want your start and stop time to appear as two BIRT parameters. To do so, you simply have to go to the `Time range parameter` pane when you are building your query and check the `Set time as a parameter option`. This creates two dataset parameters that you can bind to your report parameters in the `Parameters` tab. Go to the `Parameters` tab in the `Edit Data Set` window, select one of the two parameters you have and select `Edit`. You can configure the link to a report parameter with the `Linked to Report Parameter` field. You should then do the same with the other parameter, except if you are satisfied with just a default value. This allows you to be able to define the start and stop time of this dataset at runtime, so you can define a new time range each time you run your report without modifying your dataset configurations.

These parameters use the powerful Natty Date Parser (https://github.com/joestelmach/natty), which allows you to enter your dates in a very flexible manner. You can use standard formats such as `Thu, 04 Dec 2014 13:36:45 GMT`, but also natural language formulas such as `Yesterday at 8 am`. Only the European format that writes dates like `4th of January` as `04/01` is not supported.

# Text data type

If your series is not just numbers, you might want to look into the `Value Type` pane of the `Query` tab of the `Edit Data Set` window. By default if your time series has text values they will be set to `null`.
