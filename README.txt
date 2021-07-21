CherryPI Controllers Splunk

The mount point for custom endpoints is:

   http://localhost:8000/custom/<AppName>/

Declare a host controller for the custom endpoint in web.conf. To set custom configurations, create or edit web.conf in $SPLUNK_HOME/etc/apps/<APP_NAME>/default/ or put it inside $SPLUNK_HOME/etc/apps/<APP_NAME>/local/ 

Add the following stanza to web.conf:

   [endpoint:<unique identifier>]
   # no other content required

Place the python resources implementing the custom endpoint in the controllers directory, which is at the following location:

   $SPLUNK_HOME/etc/apps/<APP_NAME>/appserver/controllers/

Note: Changes to web.conf require a full restart; changes to python code require a splunkweb restart ($SPLUNK_HOME/bin/splunk restart splunkweb)


Example ExcelDownloader:

web.conf:
[endpoint:download]

/appserver/controllers/:
download.py

custom endpoints:
http://localhost:8000/en-US/custom/ExcelDownloader/download/excel?sid=XXX&fileName=XXX&fileType=XXX


Source: https://wiki.splunk.com/Community:40GUIDevelopment
