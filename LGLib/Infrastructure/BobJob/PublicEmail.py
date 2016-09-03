# -*- coding: utf-8 -*-
import smtplib  
from email.mime.text import MIMEText
mail_host="smtp.gmail.com"
mail_user="lglibpublic" 
mail_pass="makemoney2016"  
mail_postfix="gmail.com"
  
def send_mail(to_list,sub,content,tag):
    me             = tag+" <"+mail_user+"@"+mail_postfix+"> "
    msg            = MIMEText(content,_subtype='html',_charset='gb2312')
    msg['Subject'] = sub
    msg['From']    = me  
    msg['To']      = ";".join(to_list)  
    try:  
        s = smtplib.SMTP_SSL(mail_host, 465)  
        s.connect(mail_host)
        s.login(mail_user,mail_pass)
        s.sendmail(me, to_list, msg.as_string())
        s.close()  
        return True  
    except Exception, e:  
        print str(e)  
        return False  

