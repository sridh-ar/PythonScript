from email.mime.text import MIMEText
from datetime import datetime,timedelta
from pytz import timezone
import psycopg2
import smtplib
import os



def doTask(conn):

   EST = timezone('EST');
   YESTERDAY = datetime.now(EST).date() - timedelta(days = 1)
   DIRECTORY = '//usfl04efs00v/Test90/Output/External/Vendors/Heroku/Logs/C logs/'
   FILECOUNT = 0

   TABLENAME = 'hs_hqs.import_contractdependencyerror'
   COLUMNS = '(taxamount__c,price__c,tax2__c,externalid__c,tax1__c,contract_externalid__c,product_offer_externalid__c,error)'
   # COLUMNS = '(contract_externalid__c,product_offer_externalid__c,error)'
   db = conn.cursor()

   for file in os.listdir(DIRECTORY):
      filetime = datetime.fromtimestamp(os.path.getctime(DIRECTORY + file))
      if (filetime.date() == YESTERDAY and file.endswith('contract_line_item__c_missing_dependency.csv')):
         print('Filename: '+file)
         with open(DIRECTORY+file) as filecontent:
            db.copy_expert(f"COPY {TABLENAME+COLUMNS} FROM STDIN WITH DELIMITER '|' CSV HEADER", filecontent)
         FILECOUNT += 1

   print("Records Copied to table")

   conn.commit()
   db.execute('select count(*) from '+TABLENAME)
   RECORDSCOUNT = db.fetchone()[0]
   conn.close()

   # sendMail(TABLENAME,FILECOUNT,RECORDSCOUNT,TODAY)

   

def sendMail(tableName,filesCount,totalRecords,date):
   emailHost ='barracuda.hsus.hsa.int'
   emailPport = 25

   sender = 'sridhar.balu@homeserveusa.com'
   # receivers = ['sridhar.balu@homeserveusa.com','sairamnath.santhanam@homeserveusa.com','bala.venkatakrishnanm@homeserveusa.com','Peter.Buckley@homeserveusa.com','Saumya.Sridharan@homeserveusa.com','jayaprakashsk@bahwancybertek.com','saravanan.c@homeserveusa.com','Andrew.Burley@homeserveusa.com','elliot.griffiths@homeserveusa.com','Akesh.Puri@homeserveusa.com','Subathira.Kamaraj@homeserveusa.com','Lynn.Tomlinson@homeserveusa.com']
   receivers = ['sridhar.balu@homeserveusa.com']

   body = f"""\
   <html>
      <body><br><p>
         Dependency mismatched records are copied to {tableName} table - <b>{date}</b>
         <br><br>
           <b>Records Count: </b>{totalRecords}<br>
           <b>Files Count: </b>{filesCount}<br>
         </p>
      </body>
   </html>
"""
   msg = MIMEText(body,'html')
   msg['Subject'] = f"Dependency Mismatched Records - {date}"
   msg['From'] = 'sridhar.balu@homeserveusa.com'
   # msg['To'] = 'sairamnath.santhanam@homeserveusa.com,bala.venkatakrishnanm@homeserveusa.com,Peter.Buckley@homeserveusa.com,Saumya.Sridharan@homeserveusa.com,jayaprakashsk@bahwancybertek.com,saravanan.c@homeserveusa.com,Andrew.Burley@homeserveusa.com,elliot.griffiths@homeserveusa.com,Akesh.Puri@homeserveusa.com,Subathira.Kamaraj@homeserveusa.com,Lynn.Tomlinson@homeserveusa.com'
   msg['To'] = ''

   with smtplib.SMTP(emailHost, emailPport) as server:
      server.sendmail(sender, receivers, msg.as_string())
      print("Email sent Successfully")

UAT = psycopg2.connect(
   database="d7h2t6u5aslurn",
   user='uer3vs95vtphqm', 
   password='p8a423f3fa08f16c1cbbc796c60a1373bf140e4181bb2aecd27fb40781e226d04', 
   host='vpce-080278209aaea94b3-sxxddiff.vpce-svc-01163e5c632f7cdc1.us-east-1.vpce.amazonaws.com', 
   port= '5432'
)
print("Connection Established")
doTask(UAT)



