import logging
import logging.handlers

from os.path import abspath, dirname, join, exists
from os import makedirs

try:
    from .exchange_wrapper import send_email#, credentials
except ModuleNotFoundError as e:
    print(str(e))




class MyHTTPHandler(logging.handlers.HTTPHandler):
    def mapLogRecord(self, record):
        """
        Default implementation of mapping the log record into a dict
        that is sent as the CGI data. Overwrite in your class.
        Contributed by Franz Glasner.
        """
        _d = record.__dict__
        if 'att' in _d: del _d['att'] # если в записи пришло письмо(exchangelib Message сущность) - удаляем
        if 'ps' in _d: del _d['ps'] # доб инф поле ps тоже удаляем
        return _d

    def emit(self, record):
        """
        Emit a record.

        Format the record and send it to the specified addressees.
        """
        if hasattr(record, 'nohttp') and getattr(record, 'nohttp')==True: return
        super().emit(record)
        

# https://gist.github.com/anonymous/1379446
class BufferingExchangeHandler(logging.handlers.BufferingHandler):
    def __init__(self, to, cc=[], subject="", capacity=3000, **kw):
        logging.handlers.BufferingHandler.__init__(self, capacity)

        self.cc = cc
        self.to = to
        self.subject = subject

        fmt = kw.get('fmt', "%(asctime)s %(levelname)-15s %(message)s")
        self.setFormatter(logging.Formatter(fmt))

        self.report_to = kw.get('report_to', {})

        #loggging уровень события ниже которого не будут попадать в отчет
        self.mail_logging_level = kw.get('mail_logging_level', logging.DEBUG)

        self.mail_account = kw.get('mail_account')

    def flush(self):
        if len(self.buffer) < 1: return

        was_alarm=""

        try:
            records = [r for r in self.buffer if r.levelno>=self.mail_logging_level]
            if not records: return

            _body=""

            for record in records:
                levelno = record.levelno

                tags=('','')
                if levelno in range(logging.DEBUG, logging.INFO):
                    tags=('<font color="#454545">','</font>')
                if levelno in range(logging.INFO, logging.WARNING):
                    tags=('<font color="green">','</font>')
                if levelno in range(logging.WARNING, logging.ERROR):
                    tags=('<font color="#FFC300"><b>','</b></font>')
                    was_alarm = "[WRN]"
                if levelno in range(logging.ERROR, logging.CRITICAL):
                    tags=('<font color="red"><b>','</b></font>')
                    was_alarm = "[ERR]"
                if levelno in range(logging.CRITICAL, 60):
                    tags=('<font color="maroon"><b>','</b></font>')
                    was_alarm = "[CRITICAL]"
                
                s = self.format(record)
                _body += f"{tags[0]}{s}{tags[1]}<br>"



            to = self.to if record.levelname not in self.report_to else self.report_to[record.levelname].get('to', self.to)
            cc = self.cc if record.levelname not in self.report_to else self.report_to[record.levelname].get('cc')

            subject = self.subject if was_alarm=='' else f"{was_alarm}{self.subject}"

            color = "red" if was_alarm else 'black'
            body = f"<font color='{color}'><b>{subject}</b></font><br><hr><br>{_body}"
            
            send_email(
                    account=self.mail_account,
                    to=to,
                    cc=cc,
                    subject=subject,
                    body=body,
                    send_only=False,
                )
        except:
            self.handleError(None)  # no particular record
        self.buffer = []

        # if len(self.buffer) > 0:
            # try:
                # import smtplib
                # port = self.mailport
                # if not port:
                    # port = smtplib.SMTP_PORT
                # smtp = smtplib.SMTP(self.mailhost, port)
                # msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n" % (self.fromaddr, string.join(self.toaddrs, ","), self.subject)
                # for record in self.buffer:
                    # s = self.format(record)
                    # print s
                    # msg = msg + s + "\r\n"
                # smtp.sendmail(self.fromaddr, self.toaddrs, msg)
                # smtp.quit()
            # except:
                # self.handleError(None)  # no particular record
            # self.buffer = []


class ExchangeHandler(logging.Handler):
    """
    A handler class which sends an Exchange email for each logging event.
    """
    def __init__(self, to=[], cc=[],
                    subject=None,
                    mailhost=None,
                    credentials=None, secure=None, timeout=5.0, **kw):
        """
        Initialize the handler.

        Initialize the instance with the from and to addresses and subject
        line of the email. To specify a non-standard SMTP port, use the
        (host, port) tuple format for the mailhost argument. To specify
        authentication credentials, supply a (username, password) tuple
        for the credentials argument. To specify the use of a secure
        protocol (TLS), pass in a tuple for the secure argument. This will
        only be used when authentication credentials are supplied. The tuple
        will be either an empty tuple, or a single-value tuple with the name
        of a keyfile, or a 2-value tuple with the names of the keyfile and
        certificate file. (This tuple is passed to the `starttls` method).
        A timeout in seconds can be specified for the SMTP connection (the
        default is one second).
        """
        logging.Handler.__init__(self)
        # if isinstance(mailhost, (list, tuple)):
            # self.mailhost, self.mailport = mailhost
        # else:
            # self.mailhost, self.mailport = mailhost, None
        # if isinstance(credentials, (list, tuple)):
            # self.username, self.password = credentials
        # else:
            # self.username = None

        # if isinstance(toaddrs, str):
            # toaddrs = [toaddrs]
        self.to = to
        self.cc = cc
        self.subject = subject
        # self.secure = secure
        # self.timeout = timeout

        self.report_to = kw['report_to'] if 'report_to' in kw else {}
        self.mail_account = kw.get('mail_account')

        #для отправки на внешние адреса нужно выставить в False
        self.send_only = kw.get('send_only', False)

    def getSubject(self, record):
        """
        Determine the subject for the email.

        If you want to specify a subject line which is record-dependent,
        override this method.
        """
        # s = self.format(record)[:250]
        s = f"{record.asctime} {record.levelname} [{record.name}]"
        return self.subject if self.subject is not None else s

    def correct_record_to_cc(self, record, **kw):
        # если в extra параметре в лог-записи передан ключ to
        # необходимо добавить значение в текущее to
        # сложность что record.to может быть строкой или списком
        # и to может быть строкой или списком
        # нужно единообразить
        for varname, value in kw.items():
            if hasattr(record, varname):
                var = getattr(record, varname)

                if isinstance(var, list) and isinstance(value, list):
                    value.extend(var)
                elif isinstance(var, list) and isinstance(value, str):
                    value += ";".join(var)
                    value = value.replace(';;',';')
                elif isinstance(var, str) and isinstance(value, list):
                    value.append(var)
                elif isinstance(var, str) and isinstance(value, str):
                    value += var if not value.endswith(';') else f";{var}"
                    value = value.replace(';;',';')

                if value is None or value==[] or value=="":
                    if var is not None or var!=[] or var!="":
                        value = var

            yield value

    def printvars(self, *args, **kw):
        for i,a in enumerate(args):
            print(f"{i}\t:{repr(a)}")

        for k,v in kw.items():
            print(f"{k}\t{repr(v)}")


    def emit(self, record):
        """
        Emit a record.

        Format the record and send it to the specified addressees.
        """
        # to = self.to if record.levelname not in self.report_to else self.report_to[record.levelname].get('to')
        # cc = self.cc if record.levelname not in self.report_to else self.report_to[record.levelname].get('cc')
        if hasattr(record, 'nosend') and getattr(record, 'nosend')==True: return

        to = self.to
        cc = self.cc

        for k,v in self.report_to.items():
        # обработка на тот случай если в качестве ключа будет кортеж с несколькими событиями
        # на случай отправки одной группе лиц по нескольким событиям
            if record.levelname in k: # проверяем либо вхождение в строку либо в кортеж
                to = v.get('to')
                cc = v.get('cc')

        to,cc = self.correct_record_to_cc(record, to=to, cc=cc)

        subject = self.getSubject(record) if not hasattr(record, 'subject') else record.subject

        body = self.format(record) if not hasattr(record, 'body') else record.body
        body =  body if not hasattr(record, 'ps') else f"{body}<p><hr><i>Примечание:</i><p>{record.ps}</p></p>"

        atts = record.att if hasattr(record, 'att') and record.att is not None else []

        mail_account = record.mail_account if hasattr(record, 'mail_account') else self.mail_account
        send_only = record.send_only if hasattr(record, 'send_only') else self.send_only
        
        if hasattr(record, 'showvars'):
            showvars=getattr(record, 'showvars')
            print({k:getattr(record,k) for k in dir(record) if not k.startswith('_')})
            print(locals())
            if isinstance(showvars, (list, tuple,)):
                self.printvars(**{k:locals()[k] if k in locals() else "not in locals()" for k in showvars})
            else:
                self.printvars(**{
                    'to':to
                    ,'cc':cc
                    ,'subject':subject
                    ,'body':body
                    ,'attachments':atts
                    ,'send_only':send_only
                    ,'mail_account':mail_account
                })

        try:
            send_email(
                account=mail_account,
                to=to,
                cc=cc,
                subject=subject,
                body=body,
                attachments=atts,
                send_only=send_only,
                # send_only=False,
            )

            # port = self.mailport
            # if not port:
                # port = smtplib.SMTP_PORT
            # smtp = smtplib.SMTP(self.mailhost, port, timeout=self.timeout)
            # msg = EmailMessage()
            # msg['From'] = self.fromaddr
            # msg['To'] = ','.join(self.toaddrs)
            # msg['Subject'] = self.getSubject(record)
            # msg['Date'] = email.utils.localtime()
            # msg.set_content(self.format(record))
            # if self.username:
                # if self.secure is not None:
                    # smtp.ehlo()
                    # smtp.starttls(*self.secure)
                    # smtp.ehlo()
                # smtp.login(self.username, self.password)
            # smtp.send_message(msg)
            # smtp.quit()
        except Exception:
            self.handleError(record)


def set_custom_log_levels(config={}):
    """
        Метод назначает новые уровни логгирования
            конфиг: словарь, вида
            {
                'EVENT_NAME': EVENT_LEVEL_NUM
            }
        уровни не могут заменять существующие в модуле logging
        logging.DEBUG       = 10
        logging.INFO        = 20
        logging.WARNING     = 30
        logging.ERROR       = 40
        logging.CRITICAL    = 50
    """
    assert isinstance(config, dict), "Конфиг новых уровней логгирования может быть только словарём"

    def get_blank(level_name, level_num):
        def _blank(self, message, *args, **kws):
            if self.isEnabledFor(level_num):
                # Yes, logger takes its '*args' as 'args'.
                self._log(level_num, message, args, **kws) 
        _blank.__name__ = level_name.lower()
        return _blank

    for level_name, level_num in config.items():
        logging.addLevelName(level_num, level_name.upper())
        setattr(logging.Logger, level_name.lower(), get_blank(level_name, level_num))




def log(logfn=__file__, levelname = "DEBUG", take_path=__file__, add_folder="",
            mail_to='groshev_pp@magnit.ru', mail_cc=[], mail_logging_level=logging.ERROR,
            logfilesize=1, backupCount=5,
            new_levels = {}, buffered=False, **kw):

    log = logging.getLogger(logfn)
    level = logging.getLevelName(levelname)
    log.setLevel(level)

    fmt = kw.get('fmt', '%(asctime)s %(levelname)-9s %(message)s')
    formatter = logging.Formatter(fmt)

    _take_path = dirname(abspath(take_path))
    # _ABSPATH = dirname(abspath(__file__))
    # _ABSPATH = dirname(abspath(logfn))
    # _LOGPATH = join(_ABSPATH, 'log')
    # if not exists(_LOGPATH):
        # makedirs(_LOGPATH)
    if add_folder:
        add_folder = add_folder.strip('/') if '/' in add_folder else add_folder
        add_folder = add_folder.strip('\\') if '\\' in add_folder else add_folder
        _take_path = join(_take_path, add_folder)

    if not exists(_take_path):
        makedirs(_take_path)

    if not logfn.endswith('.log'):
        _LOGFILENAME = "%s.log" % logfn if logfn else "log"
    else:
        _LOGFILENAME = "%s" % logfn if logfn else "log"

    _LOG = join(_take_path, _LOGFILENAME)
    # _LOG = _LOGFILENAME
    # _LOG = join(_LOGPATH, _LOGFILENAME)

    _LOGFILESZ = 1024*1024*logfilesize # 1mb

    handler = logging.handlers.RotatingFileHandler(_LOG, maxBytes=_LOGFILESZ, backupCount=backupCount)

    handler.setLevel(level)
    handler.setFormatter(formatter)

    log.addHandler(handler)


    if 'stream_no' not in kw:
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(formatter)

        log.addHandler(handler)

    # EXCHANGE logger
    if 'exchange_no' not in kw:
        exchange_handler = ExchangeHandler(to=mail_to, cc=mail_cc, **kw)
        exchange_handler.setLevel(mail_logging_level)
        exchange_handler.setFormatter(formatter)

        log.addHandler(exchange_handler)


    # HTTP logger
    if 'http_no' not in kw:
        # log_server = kw.get('log_server', '127.0.0.1:5000')    # server = '127.0.0.1:31277'
        log_server = kw.get('log_server', '127.0.0.1:31277')    # server = '127.0.0.1:31277'
        # log_path = kw.get('log_path', '/test')
        log_path = kw.get('log_path', '/log/sandbox_db')
        log_method = kw.get('log_method', 'POST')    # method = 'POST'
        # для доступа в боевом режиме будем использовать данные сервисной УЗ
        # которые возьмем из exchange_wrapper
        # log_credentials = kw.get('credentials', (credentials.username, credentials.password)) 
        log_credentials = kw.get('credentials', ('CORP\svc_apteki_trading', '1DuruENTp2Y6',)) 

        # http_handler = logging.handlers.HTTPHandler(log_server, log_path, method=log_method, credentials=log_credentials)
        http_handler = MyHTTPHandler(log_server, log_path, method=log_method, credentials=log_credentials)
        http_handler.setLevel(mail_logging_level)
        # http_handler.setLevel(level)
        # http_handler.setLevel(logging.DEBUG)
        http_handler.setFormatter(formatter)

        log.addHandler(http_handler)



    if buffered:
        buffered_subject = kw.get('buffered_subject', "Протокол логирования")
        buffered_mail_to = kw.get('buffered_mail_to')
        buffered_mail_cc = kw.get('buffered_mail_cc')

        log.addHandler(
            BufferingExchangeHandler(
                buffered_mail_to
                ,buffered_mail_cc
                ,subject=buffered_subject
                ,mail_logging_level=mail_logging_level
                ,**kw
            )
        )


    if new_levels:
        set_custom_log_levels(new_levels)

    return log



def logging_shutdown():
    # используется для BufferingExchangeHandler - чтобы выггрузить все записи текущего логгирования
    logging.shutdown()



if __name__ == "__main__":
    log = logging.getLogger(__file__)
    log.setLevel(logging.DEBUG)
    fmt='%(asctime)s %(levelname)-10s %(funcName)-20s %(message)s'
    formatter = logging.Formatter(fmt)


    kw={}
    # HTTP logger
    log_server = kw.get('log_server', '127.0.0.1:5000')    # server = '127.0.0.1:31277'
    log_path = kw.get('log_path', '/test')
    log_method = kw.get('log_method', 'POST')    # method = 'POST'
    # для доступа в боевом режиме будем использовать данные сервисной УЗ
    # которые возьмем из exchange_wrapper
    # log_credentials = kw.get('credentials', (credentials.username, credentials.password)) 
    log_credentials = kw.get('credentials') 

    http_handler = MyHTTPHandler(log_server, log_path, method=log_method, credentials=log_credentials)
    http_handler.setLevel(logging.DEBUG)
    # http_handler.setLevel(level)
    # http_handler.setLevel(logging.DEBUG)
    http_handler.setFormatter(formatter)

    log.addHandler(http_handler)


    log.info('test')
