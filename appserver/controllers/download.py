import sys
import os
import io

import splunk
import splunk.search
import splunk.appserver.mrsparkle.controllers as controllers
from splunk.appserver.mrsparkle.lib.decorators import expose_page
import cherrypy
import json
import traceback

sys.path.append(os.path.join(os.environ['SPLUNK_HOME'],"etc/apps/ExcelDownloader/lib"))

import pandas as pd
#import splunklib.client as client

def log(msg):
    f = open(os.path.join(os.environ["SPLUNK_HOME"], "var", "log", "splunk", "download.log"), "a")
    f.write("%s\n" % msg)
    f.close()

class download(controllers.BaseController):
  
  @expose_page(must_login=True, methods=['GET']) 
  def excel(self, sid, fileType, fileName=None, **kwargs) :

    if not fileName:
      fileName = "%s.%s" % (sid,fileType)

    log("Creating report %s for SID %s" % (fileName, sid))

    cherrypy.response.stream = True
    cherrypy.response.headers['Content-Type'] = 'application/%s' % fileType
    cherrypy.response.headers['Content-Disposition'] = 'attachment; filename="%s"' % fileName
    try:
        csv_input = self.generateJobResults(sid)
        log("Generate Job Results")
        return self.outputResults(csv_input, fileType)
    except Exception as e:
        log(traceback.format_exc(),e)
        return None

  @expose_page(must_login=True, methods=['GET']) 
  def email(self, emailFileName, emailFrom, emailTo, emailCc, emailSubject, emailBody, attachmentSid=None, attachmentFileName=None, attachmentFileType=None, **kwargs) :

    from email.message import EmailMessage
    from email.headerregistry import Address
    import html2text

    log("Creating email %s" % emailFileName)

    cherrypy.response.stream = True
    cherrypy.response.headers['Content-Type'] = 'application/octet-stream'
    cherrypy.response.headers['Content-Disposition'] = 'attachment; filename="%s"' % emailFileName

    log("Creating email")
    log("EmailFrom:%s;EmailTo:%s,EmailCC:%s" % (emailFrom,emailTo,emailCc))

    msg = EmailMessage()
    msg['Subject'] = emailSubject
    msg['From'] = emailFrom
    msg['To'] = emailTo
    msg['Cc'] = emailCc
    msg['X-Unsent']="1"

    msg.set_content(html2text.html2text(emailBody))
    msg.add_alternative("""
    <html>
      <body>
        %s
      </body>
    </html>
    """ % emailBody, subtype='html')
    
    if attachmentSid:
      if not attachmentFilename:
        attachmentFilename = "%s.%s" % (attachmentSid, attachmentFileType)
      csv_input = self.generateJobResults(attachmentSid)
      msg.add_attachment(outputResults(csv_input,attachmentFileType), maintype="application", subtype="octet-stream", filename=attachmentFileName)
    log(msg)
    log("Done")
    return bytes(msg)

  def generateJobResults(self, sid, **kwargs):
    log("Connecting to Splunk...")
    try:
      sessionKey=cherrypy.session['sessionKey']
      job = splunk.search.getJob(sid, sessionKey=sessionKey)
      
      #Alternative way using splunklib:
      #service = client.connect(token=sessionKey)
      #job2 = client.Job(service, sid)
    
    except:
      raise cherrypy.HTTPError('400', 'sid not found')
      
    while not job.isDone:
      time.sleep(.1)

    log("Retrieving results...")
    resultCount = job.resultCount
    offsetValue = 0
    countValue = 49999
    results = ""
    while offsetValue<resultCount:
        search_raw = job.getFeed(mode="results", output_mode="csv", count=countValue, offset=offsetValue)
        results += search_raw.decode("utf-8")
        offsetValue += countValue
    csv_input = io.StringIO(results)
    return csv_input

  def outputResults(self, csv_input,fileType, **kwargs):
    if fileType=="csv":
      return csv_input
    elif fileType=="xlsx":
      excel_output = io.BytesIO()
      log("Reading csv results")
      df = pd.read_csv(csv_input).fillna(value="")
      log("Parsing to xlsx")
      df.to_excel(excel_output,index=False)
      log("Done")
      return excel_output.getvalue()
