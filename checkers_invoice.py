from sys import path as syspath
syspath.insert(0, 'e:\\ma\\email_import')

# KОМАНДА РАЗВИТИЯ ИНФОРМАЦИОННЫХ СИСТЕМ АПТЕК

# [ТЕМА]
# Отправлять информирование на КА если в накладной не заполнены данные

# [ССЫЛКА]
# https://localjira/browse/TESTAPTEK-1962

# [ОПИСАНИЕ]
# https://localjira/browse/TESTAPTEK-1962

# [РЕАЛИЗАЦИЯ]
#

""" Накладная - класс Invoice,
    которая содержит набор (класс InvoiceList, клон list) записей классов InvoiceRecord
    InvoiceRecord - это одна строка накладной (все столбцы и их значения), доступ через точку, 
        напр inv[0].BILLDT - из первой строки накладной получить значение столбца BILLDT

    Invoice и InvoiceList позволяет выборки по срезам, напр
        >>> i[:4].TITLE          # получаем список значений TITLE с первых 4х строк накладной
        ['АСКОРБИНОВАЯ КИСЛОТА С ГЛЮКОЗОЙ ТАБ. 0,1Г №10', 'АЦИКЛОВИР-АКРИХИН МАЗЬ Д/НАР. ПРИМ. 5% ТУБА 10Г', 'БАРСУКОР БАРСУЧИЙ ЖИР ЖИДКИЙ ФЛ. 100МЛ (БАД)', 'БРОНХО-МУНАЛ П КАПС. 3,5МГ №10']

        >>>  i.records.TITLE     # получаем список значений TITLE со всех строк накладной, то же что и i[:].TITLE
        [...]

        >>>repr(i)
        Накладная(всего строк=30, пустые значения=0, ошибок=0)      # строковое представление накладной

        >>>len(i)
        30                       # количество строк в накладной

        >>>i.empty               # Незаполненные столбцы, список кортежей (номер строки, список[название столбцов])
        [(1, ['NUMGTD']), (6, ['NUMGTD'])]
"""

from collections import UserList
from collections import Counter

from datetime import date


from utils.log import log
log = log("invoice_checker", "DEBUG", take_path=__file__, add_folder=''
    # ,mail_account = apteki_ext_account
    ,exchange_no=True, http_no=True
    ,stream_no=True
    # ,mail_to=error_report_addr['to'], mail_cc=error_report_addr['cc']
    # ,new_levels = new_log_levels_alert_accepts_expired
    # ,report_to = report_to_alert_accepts_expired
    ,logfilesize=10 # размер лог-файлов
    ,backupCount=3 # количество лог-файлов
    # ,fmt='%(asctime)s %(message)s'
    ,fmt='%(asctime)s %(levelname)-10s %(funcName)-20s %(message)s'
    # ,mail_logging_level = 25 #чуть выше logging.INFO см. в config
    ,buffered=False
    # ,buffered_subject = "[LOG] Результат рассылки уведомлений об отсутствующих подтверждениях"
    # ,buffered_mail_cc = 'logapteka@magnit.ru;groshev_pp@magnit.ru'
    # ,buffered_mail_to = 'abramov_mu@magnit.ru;savkin_kv@magnit.ru'
    # ,buffered_mail_to = 'rudakova_s_v@magnit.ru'
    # ,buffered_mail_to = 'groshev_pp@magnit.ru'

)

from sys import excepthook as sysexcepthook
from utils.handle_exception import handle_exception
sysexcepthook = handle_exception




from utils.incorderdict.incorderdict import IncrementalOrderedDict

try:
    from utils.exchange_wrapper import send_email, apteki_orders_account
except ModuleNotFoundError:
    send_email=None


from config import valid_barcode_lengths, vat_rates, \
    invoice_check_columns, invoice_excepted_from_empty_check \
    ,checkers_invoice_duplicate_lines_check_columns





class InvoiceCheckRules(object):
    """Набор проверок для строки накладной, 
        здесь все проверки для каждого столбца для ВСЕХ(!) СТРОК накладной, кроме проверки пустых значений"""

    @staticmethod
    def NDOC(invoice):
        """Столбец должен содержать одинаковые значения"""
        return not bool(invoice.NDOC.all_duplicates) # инвертируем результат

    @staticmethod
    def DATEDOC__01(invoice):
        """Столбец должен содержать одинаковые значения"""
        return not bool(invoice.DATEDOC.all_duplicates) # инвертируем результат
    
    @staticmethod
    def DATEDOC__02(invoice):
        """Поле не типа "DATE", должно содержать дату, вида ДД.ММ.ГГГГ"""
        res=[]
        for invoicelineno, invoicedata in invoice.items():
            val=invoicedata.DATEDOC
            if not isinstance(val, date):
                res.append(invoicelineno)

        return f", ошибки в строках: {','.join([str(r) for r in res])}" if res else False

    @staticmethod
    def EAN13__01(invoice):
        """Обнаружены ШК неверной длины"""
        res=[]
        for invoicelineno, invoicedata in invoice.items():
            val=invoicedata.EAN13
            if len(val) not in valid_barcode_lengths:
                res.append( (invoicelineno, len(val),) )

        return f", ошибки в строках: {'; '.join([f'{line}: длина ШК:{l}' for line, l in res])}" if res else False


    @staticmethod
    def EAN13__02(invoice):
        """Обнаружены ШК которые не содержат чисел"""
        res=[]
        for invoicelineno, invoicedata in invoice.items():
            val=invoicedata.EAN13
            if not all([n in '0123456789' for n in val]):
                res.append(invoicelineno)

        return f", ошибки в строках: {','.join([str(r) for r in res])}" if res else False

    @staticmethod
    def EAN13__03(invoice):
        """Обнаружены ШК которые содержат только нули"""
        res=[]
        for invoicelineno, invoicedata in invoice.items():
            val=invoicedata.EAN13
            if all([n=='0' for n in val]):
                res.append(invoicelineno)

        return f", ошибки в строках: {','.join([str(r) for r in res])}" if res else False

    @staticmethod
    def PRICE1(invoice):
        """Для товаров с признаком 1 в поле GNVLS не заполнен PRICE1"""
        x=list(zip(invoice.GNVLS, invoice.PRICE1)) # собираем все PRICE1 и GNVLS (массив кортежей)
        g=list(filter(lambda k: k[0]==1, x)) # выбираем только которые GNVLS==1
        # a=list(map(lambda k: k[1]>0, g)) # проверяем, все из них PRICE1 > 0
        res=list(filter(lambda k: k[1]==0, g)) # проверяем, все из них PRICE1 > 0

        return f", ошибки в строках: {','.join([str(r) for r in res])}" if res else False
        # return not all(a) # инвертируем


    @staticmethod
    def VAT(invoice):
        """Значение ставки НДС содержит неверное значение"""
        res = [(v,i) for i,v in enumerate(invoice.VAT,1) if v not in vat_rates]
        i=IncrementalOrderedDict(res)
        i.keyname='в строке(-ах)'
        vat_errs_list = f", {str(i)}"
        vat_errs_list += f" ,допустимые:{','.join([str(v) for v in vat_rates])}"
        return vat_errs_list if res else False
        # ('VAT', 'Значение ставки НДС содержит неверное значение, список: ставка:10, в строках:1,2,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,20,21,22,24,25,26,27,28,29,30,31 ,допустимые:0,18,20')

    @staticmethod
    def REGPRC(invoice):
        """Поле обязательно для заполнения только для товаров с признаком 1 в поле GNVLS"""
        x=list(zip(invoice.GNVLS, invoice.REGPRC)) # собираем все в массив кортежей
        g=list(filter(lambda k: k[0]==1, x)) # выбираем только которые GNVLS==1
        # a=list(map(lambda k: k[1]>0, g)) # проверяем, все из них > 0

        res=list(filter(lambda k: k[1]==0, g)) # проверяем, все из них > 0
        # return not all(a) # инвертируем
        return f", ошибки в строках: {','.join([str(r) for r in res])}" if res else False

    # @staticmethod
    # def SERTGIVE(invoice):
        # """Поле должно быть типа DATE и содержать дату в виде ДД.ММ.ГГГГ"""
        # # return not all([isinstance(s, date) for s in invoice.SERTGIVE])
        # res=[(v,i) for i,v in invoice.SERTGIVE.lineno_value() if not isinstance(v, date)]
        # i=IncrementalOrderedDict(res)
        # i.keyname='в строке(-ах)'
        # return f", {str(i)}" if res else False
        

    # @staticmethod
    # def SERTGIVE__01(invoice):
        # """Неверное значение - Дата не может быть в будущем"""
        # res = [(v,i) for i,v in invoice.SERTGIVE.lineno_value() if isinstance(v, date) and v>date.today()]
        # i=IncrementalOrderedDict(res)
        # i.keyname='в строке(-ах)'
        # return f", {str(i)}" if res else False

    @staticmethod
    def DATEZ(invoice):
        """Поле не типа DATE, должно содержать дату, вида ДД.ММ.ГГГГ"""
        # return not all([isinstance(s, date) for s in invoice.DATEZ])
        res=[(v,i) for i,v in invoice.DATEZ.lineno_value() if not isinstance(v, date)]
        i=IncrementalOrderedDict(res)
        i.keyname='в строке(-ах)'
        return f", {str(i)}" if res else False

    @staticmethod
    def GNVLS(invoice):
        """Неверный тип данных - может быть только целое число"""
        # return not all([isinstance(s, int) for s in invoice.GNVLS])
        res=[(type(v),i) for i,v in invoice.GNVLS.lineno_value() if not isinstance(v, int)]
        i=IncrementalOrderedDict(res)
        i.keyname='в строке(-ах)'
        return f", {str(i)}" if res else False

    @staticmethod
    def GNVLS__01(invoice):
        """Неверное значение - признак ЖВ может быть только 0 или 1"""
        # return not all([s in [0,1] for s in invoice.GNVLS])
        res=[(repr(v),i) for i,v in invoice.GNVLS.lineno_value() if v not in [0,1]]
        i=IncrementalOrderedDict(res)
        i.keyname='в строке(-ах)'
        return f", {str(i)}" if res else False

    @staticmethod
    def CODEMAG(invoice):
        """Код номенклатуры Тандер должен содержать только цифры"""
        # return all([all([n in '0123456789' for n in val]) for val in invoice.CODEMAG])
        res = [(val,i) for i,val in invoice.CODEMAG.lineno_value() if not all([n in '0123456789' for n in val])]
        i=IncrementalOrderedDict(res)
        i.keyname='в строке(-ах)'
        return f", {str(i)}" if res else False

    @staticmethod
    def CODEMAG__01(invoice):
        """Длина кода номенклатуры Тандер должна быть 10 чисел"""
        # return all([all([n in '0123456789' for n in val]) for val in invoice.CODEMAG])
        res = [(val,i) for i,val in invoice.CODEMAG.lineno_value() if len(val)!=10]
        i=IncrementalOrderedDict(res)
        i.keyname='в строке(-ах)'
        return f", {str(i)}" if res else False

    @staticmethod
    def CODEPST(invoice):
        """Код товара поставщика содержит незаполненные строки"""
        if bool(invoice.CODEPST.has_empty): 
            res = [(val,i) for i,val in invoice.CODEPST.lineno_value()]
            return InvoiceCheckRules.strstat(res)
        return False

    @staticmethod
    def PODRCD(invoice):
        """Код подразделения не может содержать пустые строки"""
        if bool(invoice.PODRCD.has_empty): 
            res = [(val,i) for i,val in invoice.PODRCD.lineno_value()]
            return InvoiceCheckRules.strstat(res)
        return False

    @staticmethod
    def PODRCD_01(invoice):
        """Код подразделения содержит неодинаковые значения"""

        # все значения одинаковые - ошибок нет
        if bool(invoice.PODRCD.all_duplicates): return False 
        
        res = [(val,i) for i,val in invoice.PODRCD.lineno_value()]
        return InvoiceCheckRules.strstat(res)



    @staticmethod
    def strstat(res, keyname='в строке(-ах)'):
        """Вход:список кортежей(знач,номерстроки) Возвр. строку, список ошибок и номера строк в которых ошибка--напр., ошибки в строках: 1: длина ШК:14"""
        i=IncrementalOrderedDict(res)
        i.keyname=keyname
        return f", {str(i)}"





class InvoiceCheck(InvoiceCheckRules):
    """engine-класс проверок всех строк накладной"""

    def __init__(self, invoice):
        self.errors=[]
        self.empty=[]
        self.invoice = invoice

    def __call__(self):


        empty = self.check_invoice_for_empty()

        if empty:
            log.error(f"Пустые данные в накладной {empty}")

        errors = self.check_invoice_errors()

        # если накладная имеет пустые ячейки в столбцах, 
        # дальше ничего не проверяем
        # if not self.empty: 
            # self.check_invoice_rules()

        # self.invoice.empty = self.empty
        # self.invoice.errors = self.errors
        return empty, errors

    @property
    def rules(self):
        """Набор правил проверки - формируется из названий свойств self 
            которые полностью совпадают с названием столбцов накладной"""
        res=[]
        attrs=dir(self)

        for k in self.invoice.keys:
            attr = [a for a in attrs if a.startswith(k) and len(a.split('__')[0])==len(k)]

            res.extend(attr)

        return tuple(res)

    def check_invoice_errors(self):
        _r=[]
        for rulename in self.rules:
            rule = getattr(self, rulename)
            log.debug(f"Проверяем правило: {rulename}; функция?{callable(rule)}")

            if callable(rule):
                res = rule(self.invoice)
            else:
                res = rule

            # log.debug(f"{rule}-> {repr(res)}")
            if res: # правило вернуло True, значит правило нашло ошибку - фиксируем
                description = rule.__doc__

                if isinstance(res, str): # если проверка вернула строку + это часть описания ошибки
                    description += res

                _t=(rulename, description,)
                _r.append(_t)

                log.error(f"ОШИБКА, правило:{rulename} -> {description}")

        return _r

    def check_invoice_for_empty(self):
        _r=[]
        for lineno, invoice in self.invoice.items():
            if invoice.has_empty:
                _t = (lineno, invoice.empty,)
                _r.append(_t)

        return _r

    

class InvoiceRecord(object):
    """Одна строка накладной, входом может быть модель sqlalchemy или словарь"""

    def __init__(self, record, check_columns=[], empty_no_check_list=[], lineno=None, **kw):
        """record=словарь(строка накладной)"""
        """excepted_columns==столбцы которые нужно пропустить(напр, проверяем не все столбцы)"""
        """empty_no_check_list==набор столбцов которые не учитываются при подсчете пустых столбцов"""
        """lineno==номер строки накладной(начинается с 1)"""

        self.invoicelineno=lineno # номер строки в накладной, НАЧИНАЯ с 1!!!
        self._empty_no_check_list=empty_no_check_list

        if isinstance(record, dict):
        # if 'dict' in type(record).lower():
            _keys = record.keys()
        elif hasattr(record, '__table__'):
            _keys = record.__table__.c.keys()
        else:
            raise ValueError(f"Строка накладной неподдерживаемого типа(sqlalchemy_model|dict)! {type(record)};{dir(record)}")

        self.keys = tuple([k for k in _keys if k in check_columns])

        self._set_properties(record, check_columns)
        # self.values = [getattr(self, k) for k in self.keys]

    def _set_properties(self, record, check_columns=[]):
        for k in self.keys:
            if k not in check_columns: continue

            try:
                record_value=getattr(record, k)
            except AttributeError: # если это словарь а не модель sqlalchemy
                record_value = record[k]

            setattr(self, k, record_value)

    def __getitem__(self, key):
        "Доступ по ключу = InvoiceRecord['NDOC']"
        return getattr(self, key)

    def __iter__(self):
        return iter([t for t in self.items()])

    def __repr__(self):
        return str([(k,v,) for k,v in self.items()])

    def items(self, keyslist:list=[]):
        keys = self.keys if not keyslist else keyslist
        return iter([(k, getattr(self, k)) for k in keys])

    def values(self, keyslist:list=[]):
        for k,v in self.items(keyslist):
            yield v

    @property
    def empty(self):
        return [k for k,v in self.items() if k not in self._empty_no_check_list and (v is None or v=="")]

    @property
    def has_empty(self):
        return bool(self.empty)

    # @classmethod
    # def get_cls(cls, record, excepted_columns=[]):
        # cls.keys = [k for k in record.__table__.c.keys() if k not in excepted_columns]
        # for k in cls.keys:
            # if k in excepted_columns: continue

            # record_value=getattr(record, k)
            # setattr(cls, k, record_value)

        # return cls


class InvoiceList(UserList):
    """Массив строк накладной == список классов InvoiceRecord"""
    def __init__(self, l=[]):
        super().__init__(l)

    def __getattr__(self, key):
        return type(self)([getattr(self[i], key) for i in range(len(self))])

    @property
    def has_empty(self):
        return "" in self or None in self

    @property
    def has_duplicates(self):
        "Кроил для использования Invoice.NDOC.has_duplicates == проверка все ли дубликаты в стобце NDOC"
        return len(self) != len(set(self))

    @property
    def all_duplicates(self):
        return len(set(self))==1

    def lineno_value(self):
        """Итератор возвр Номер строки(!)нач с 1, и значение"""
        return iter([(i,v) for i,v in enumerate(self,1)])



class Invoice(object):
    """Вся накладная,"""
    """свойства: is_buggy(bool) флаг(есть ли ошибки), """

    def __init__(self, invoicedata=None
            ,check_columns=invoice_check_columns
            ,empty_no_check_list=invoice_excepted_from_empty_check
            ,checkcls=InvoiceCheck
            ,dupcheck=checkers_invoice_duplicate_lines_check_columns
            ,**kw):

        self.keys=None

        self.check_columns=check_columns
        self.empty_no_check_list=empty_no_check_list

        self.records = InvoiceList()

        # для инф, может быть передано имя файла из которого была загружена накладная
        filename = kw.get('filename','').replace('\\','/')
        
        # if filename!='': 
            # self._load_file(filename)
        if invoicedata is None: 
            raise ValueError("Невозможно инициализировать класс - не передан массив данных(invoicedata)")
        else:
            self._load_data(invoicedata)
            #сохраним хэш ВСЕХ данных каждой строки
            # self._hashes=[hash(tuple(a)) for a in self.records]
            # сохраним хэш ВЫБРАННЫХ/УКАЗАННЫХвКОНФИГЕ столбцов каждой строки - для проверки дублирующихся
            self._hashes=tuple([hash(tuple( a.items(dupcheck) )) for a in self.records])

        log.info(f"Загружена накладная, размер {len(self)} строк {filename}")
        # log.debug(f"Исключены столбцы: {repr(self.excepted_columns)}")
        log.debug(f"Столбцы не участвующие в проверке пустых: {repr(self.empty_no_check_list)}")
        log.debug(f"Проверяем столбцы: {repr(self.keys)}")

        self.errors = []    # массив обнаруженных ошибок в накладной
        self.empty = []     # массив пустых столбцов обнаруженных в накладной

        # инициируем проверку накладной классом checkcls
        if checkcls is not None:
            self.check(checkcls)


    def check(self, checkcls):
        """Проверяем накладную предоставленным чекер-классом"""
        # log.debug(f"Проверяем через '{checkcls.__name__}'")
        checker=checkcls(self)
        empty, errors = checker()

        self.empty = empty
        self.errors = errors

        log.info(f"Результат проверки накладной: {repr(self)}")
        # del checker
        # log.debug(f"empty: {len(empty)}; errors: {len(errors)}")
        # return empty, errors

    @property
    def is_buggy(self):
        """Флаг, есть ли в накладной ошибки или пустые значения == в общем любые обнаруженные аномалии"""
        return bool(self.errors or self.empty)
        # return bool(self.errors or self.empty or self.completely_similar_lines_has)

    def _load_data(self, invoicedata):
        for index, record in enumerate(invoicedata, 1):
            # super().__init__(record, excepted_columns)
            # cls = super().get_cls(record, excepted_columns)
            invoice_record = InvoiceRecord(record, self.check_columns, self.empty_no_check_list, lineno=index)
            self.records.append(invoice_record)

            if not self.keys: 
                self.keys=invoice_record.keys

    def __getattr__(self, key):
        return type(self.records)([getattr(self.records[i], key) for i in range(len(self.records))])

    def __getitem__(self, key):
        if isinstance(key, slice): # если пришел срез
            return type(self.records)(self.records[key])

        return self.records[key]
    
    def __repr__(self):
        return f"Накладная("\
            f"всего строк={len(self)}, "\
            f"пустые значения={len(self.empty)}, "\
            f"ошибок={len(self.errors)}, "\
            f"дубли строк={'да' if self.completely_similar_lines_has else 'нет'}"\
            ")"

    def __len__(self):
        return len(self.records)

    def items(self):
        return iter([(index+1, self[index],) for index in range(len(self))])

    @property
    def document_number(self):
        """Вернем строку, номер документа поставщика по накладной"""
        # теперь подсчитвается количество вариантов(на случай если есть незаполненные данные в накл) и выбирается тот которого больше
        res=Counter(self.NDOC).most_common(1)[0][0] #https://docs.python.org/3/library/collections.html#collections.Counter.most_common
        return str(res)

        # n = [nd for nd in self.NDOC if nd is not None]
        # n = list(set(n)) # возьмем все значения, удалим дубли и вернем список
        # return n[0] if len(n)==1 else ";".join([str(nn) for nn in n])

    @property
    def document_date(self):
        """Вернем строку, дату документа поставщика по накладной"""
        # теперь подсчитвается количество вариантов(на случай если есть незаполненные данные в накл) и выбирается тот которого больше
        res=Counter(self.DATEDOC).most_common(1)[0][0] #https://docs.python.org/3/library/collections.html#collections.Counter.most_common
        return str(res)
        # n = [nd for nd in self.DATEDOC if nd is not None] # удалим пустошит
        # n = list(set(n)) # возьмем все значения, удалим дубли и вернем список
        # return str(n[0]) if len(n)==1 else ";".join([str(nn) for nn in n])

    @property
    def document_order_number(self):
        """Вернем строку, номер заказа по накладной"""
        # теперь подсчитвается количество вариантов(на случай если есть незаполненные данные в накл) и выбирается тот которого больше
        res=Counter(self.NUMZ).most_common(1)[0][0] #https://docs.python.org/3/library/collections.html#collections.Counter.most_common
        return str(res)

        # n = [nd for nd in self.NUMZ if nd is not None] # удалим пустошит
        # n = list(set(n)) # возьмем все значения, удалим дубли и вернем список
        # return str(n[0]) if len(n)==1 else ";".join([str(nn) for nn in n])

    @property
    def document_order_date(self):
        """Вернем строку, дату заказа по накладной"""
        # теперь подсчитвается количество вариантов(на случай если есть незаполненные данные в накл) и выбирается тот которого больше
        res=Counter(self.DATEZ).most_common(1)[0][0] #https://docs.python.org/3/library/collections.html#collections.Counter.most_common
        return str(res)

        # n = [nd for nd in self.DATEZ if nd is not None] # удалим пустошит
        # n = list(set(n)) # возьмем все значения, удалим дубли и вернем список
        # return str(n[0]) if len(n)==1 else ";".join([str(nn) for nn in n])

    @property
    def completely_similar_lines_has(self):
        """Имеет ли накладная полностью похожие строки"""
        return len(self) != len(set(self._hashes))

    @property
    def completely_similar_lines_get(self):
        """Вернуть список полностью похожих строк накладной. Значения чел.чит. строки накладной(нач. с 1)"""
        # https://stackoverflow.com/questions/9835762/how-do-i-find-the-duplicates-in-a-list-and-create-another-list-with-them
        # dupes = [x for n, x in enumerate(i._hashes) if x in i._hashes[n:]]
        _r=[]
        for n,x in enumerate(self._hashes,1):
            if x in self._hashes[n:]:
                _r.append(n)
                i=self._hashes[n:].index(x) + n + 1 # доб.сдвиг для получения чел.чит. номеров строк
                _r.append(i)

        return _r



# data=load_invoice()
# i=Invoice(data)z

#  >>> i[:4].TITLE          # получаем список значений TITLE с первых 4х строк накладной
# ['АСКОРБИНОВАЯ КИСЛОТА С ГЛЮКОЗОЙ ТАБ. 0,1Г №10', 'АЦИКЛОВИР-АКРИХИН МАЗЬ Д/НАР. ПРИМ. 5% ТУБА 10Г', 'БАРСУКОР БАРСУЧИЙ ЖИР ЖИДКИЙ ФЛ. 100МЛ (БАД)', 'БРОНХО-МУНАЛ П КАПС. 3,5МГ №10']

#  >>>  i.records.TITLE     # получаем список значений TITLE со всех строк накладной
# [...]

# >>> len(i)                # количество строк накладной
# 31

# >>> i.keys                # список названий полей накладной
# ['NDOC', 'DATEDOC', 'EAN13', 'PRICE1', 'PRICE2', 'PRICE2N', 'QUANTITY', 'SER', 'DATEREAL', 'TITLE', 'COUNTRY', 'FIRM', 'VAT', 'REGPRC', 'NUMGTD', 'SERTNUM', 'SERTEND', 'SERTORG', 'SERTGIVE', 'SUMSTR', 'SUMOFVAT', 'SUMEXVAT', 'TOTAL', 'TOTAL2', 'VATTOTAL', 'PODRCD', 'NUMZ', 'DATEZ', 'BILLNUM', 'BILLDT', 'GNVLS', 'Supplier_ID', 'CODEMAG']

# >>> i.empty               # список ошибок
# [(1, ['NUMGTD']), (2, ['NUMGTD']), (3, ['NUMGTD']), (5, ['NUMGTD']), (7, ['NUMGTD']), (11, ['NUMGTD']), (12, ['NUMGTD']), (17, ['NUMGTD']), (18, ['NUMGTD']), (19, ['NUMGTD']), (20, ['NDOC']), (21, ['NUMGTD']), (22, ['NUMGTD']), (23, ['NUMGTD']), (24, ['NUMGTD', 'SERTORG']), (30, ['NUMGTD'])]

# >>> i.completely_similar_lines_get
# [1, 2, 27, 28, 38, 39]


class InvoiceErrorInformer(object):
    """Отправка уведомлений об ошибках в накладной"""

    def __init__(self, send_email_func=send_email, **kw):
        """~send_email_func==функция отправки емайл сообщения
        to==емайл адрес для отправки уведомлений об ошибках в накладной, cc==копия письма
        attachments==вложение в отправку(напр. изначальное письмо с накладной от поставщика)"""

        if not callable(send_email_func):
            raise ValueError(f"Отправка емайл сообщения задана не функцией - {type(send_email_func)}. Отправка невозможна")
        else:
            self.send_email = send_email_func

        #массив данных для загрузки накладной может быть задан в параметре invoicedata
        if 'invoicedata' in kw:
            invoicedata=kw.get('invoicedata')
            if isinstance(invoicedata, (list, tuple,)):
                filename = kw.get('filename','')
                self.invoice = Invoice(invoicedata=invoicedata, filename=filename)
        else:
            self.invoice=kw.get('invoice')

    def send_notify(self, to, cc=None, attachments=[], **kw):
        if to is None:
            raise ValueError("Необходимо указать емайл получателя уведомлений, параметр to")

        try:
            self.send_email(
                    account=kw.get('account'),    # если задан account, иначе отправляем с ГПЯ apteki-ext@magnit.ru
                    to=to
                    ,cc=cc
                    ,subject=self.subject
                    ,body=self.body
                    ,attachments=attachments
                    ,send_only=False
                )
        except Exception as e:
            log.critical(f"Ошибка отправки оповещения! {repr(e)}")
            raise
        else:
            log.info(f"Отправлено уведомление на: {to}(копия:{cc})")


    @property
    def subject(self):
        dod = f", от {self.invoice.document_order_date}" if self.invoice.document_order_date is not None else ""
        subject = """Обнаружены ошибки в накладной """\
                f"""№{self.invoice.document_number} от {self.invoice.document_date}, """\
                f"""заказ №{self.invoice.document_order_number}{dod}"""
        return subject

    @property
    def body(self):
        body = f"""<p style="font-size:1.1em">Уважаемые партнеры!</p><br>

От вас получена накладная для Магнит Аптек в которой обнаружены ошибки -<br>
<b style="color:red;">накладная НЕ импортирована!</b><br><br>
Ошибки необходимо исправить и повторно отправить накладную
<p>Сводка ошибок: <b>{self.invoice}</b></p>
<p>Накладная с обнаруженными ошибками находится во вложении этого письма</p>
<hr>
<u>ВАЖНО!</u><br>
<i><b>Убедительная просьба отправить накладную в ближайшее время</i>
<hr>
<br>
<p>{self.invoice_empty_table}</p><br>
<p>{self.invoice_errors_table}</p><br>
<p>{self.invoice_completly_similar_lines_table}</p><br>
<p style="font-size:0.7em">
Письмо сгенерировано автоматически роботом уведомлений Магнит Аптек
<br>При возникновении вопросов необходимо контактировать с закрепленным менеджером
</p>
"""
        return body

    @property
    def invoice_completly_similar_lines_table(self):
        """Генерим HTML код таблицы со списком дубликатов строк"""
        if not self.invoice.completely_similar_lines_has:
            return '<div style="font-size:0.85em;color:gray;">В накладной нет дублирующихся строк</div>'

        csm = ", ".join([str(i) for i in self.invoice.completely_similar_lines_get])
        lines = f"<tr><td>{csm}</td></tr>"
        table = f"""<table border="1" style="font-size:0.7em;table-layout: auto;width: 100%;">
        <caption>Дублирующиеся строки в накладной:</caption>
    <tr>
        <th>Список номеров дублирующихся строк</th>
    </tr>
    {lines}
</table>"""
        return table

    @property
    def invoice_empty_table(self):
        """Генерим HTML код таблицы со списком незаполненных столбцов"""
        if self.invoice.empty:
            empty = self.invoice.empty
        else:
            return '<div style="font-size:0.85em;color:gray;">В накладной нет незаполненных столбцов</div>'

        lines=""
        for lineno, empty_columns_list in empty:
            if len(empty_columns_list)>1:
                lines += f"<tr><td>{lineno}</td><td>{', '.join(empty_columns_list)}</td></tr>"
            else:
                lines += f"<tr><td>{lineno}</td><td>{empty_columns_list[0]}</td></tr>"

        table = f"""<table border="1" style="font-size:0.7em;table-layout: auto;width: 100%;">
        <caption>Незаполненные столбцы в накладной:</caption>
    <tr>
        <th>№ строки накладной</th>
        <th>Названия незаполненных столбцов обязательных к заполнению</th>
    </tr>
    {lines}
</table>"""
        return table

    @property
    def invoice_errors_table(self):
        """Генерим HTML код таблицы со списком столбцов c ошибками"""
        if self.invoice.errors:
            errors = self.invoice.errors
        else:
            return '<div style="font-size:0.85em;color:gray;">В накладной нет столбцов с ошибками</div>'

        lines=""
        for column_w_error_name, error_text in errors:
            # название столбца созд из названия метода в проверке накладной
            # берем название без дандера
            if '__' in column_w_error_name: column_w_error_name=column_w_error_name.split('__')[0]
            lines += f"<tr><td>{column_w_error_name}</td><td>{error_text}</td></tr>"

        table = f"""<table border="1" style="font-size:0.7em;table-layout: auto;width: 100%;"><caption>Столбцы с ошибками в накладной:</caption>
    <tr>
        <th>Название столбца накладной</th>
        <th>Описание обнаруженной ошибки</th>
    </tr>
    {lines}
</table>"""
        return table
