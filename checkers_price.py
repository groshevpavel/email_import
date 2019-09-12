# https://localjira/browse/TESTAPTEK-1829
#
# При получении прайса от КА проводить проверки корректности прайса, и в случае выявления ошибок,
# проводить информирование КА согласно ошибкам с расшифровкой ниже. 
# Важно! загрузку прайса на площадку производить, но без позиции, с ошибкой.
#
# Ошибка 	                                        Информировать КА (ДА/НЕТ) 	    Краткое описание ошибки
#
# По позиции отсутствует ШК 	                    ДА 	                            По позиции нет ШК
# ШК не равен 8 или 13 символам 	                ДА 	                            ШК не корректный (ни 8 или 13 символов)
# Не корректная кодировка текста в прайсе 	        ДА 	                            Кодировка текста в прайсе не корректная
# Отсутстует срок годности по позиции 	            ДА 	                            По позиции отсутствует срок годности
# Срок годности по позиции менее сегодняшней дате   ДА 	                            Позиция в прайсе с истекшим Сроком Годности
# Не корректый формат прайса 	                    ДА 	                            Формат прайса не соответствует правилам обмена.
# В поле ШК присутствуют буквенные символы 	        ДА 	                            В поле ШК присутствуют буквенные символы
#
#
# Отправка емайл-уведомления поставщику НЕ производится из этого модуля!



# from utils.mydt import get_now#, from_now, str_datetime
from datetime import date



from utils.log import log
log = log("check_price", "DEBUG", take_path=__file__, add_folder=''
    # ,mail_account = apteki_ext_account
    ,exchange_no=True, http_no=True, stream_no=True
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

import sys
from utils.handle_exception import handle_exception
sys.excepthook = handle_exception

from collections import Counter

from utils.exchange_wrapper import minfo

from config import valid_barcode_lengths




class CheckerPriceRules(object):
    """
    =====================================================
      НАБОР   правил проверки одной записи прайс-листа
              + геттеры для получения нужного значения 
              из одной записи
    =====================================================
    
    все правила возвращают True если правило НЕ прошло проверку - то есть со строкой есть проблема
    иначе False - все хорошо, правило прошло проверку
    
    порядок проверки правил устанавливается цифрой в названии метода
    каждый "десяток" проверка одного из параметров
    01, 02, 03,.. - проверки ШК; 10,11,..- проверка срока годности и тд
    
    doc string метода это текстовое описание обнаруженной ошибки, 
    попадёт в лог ошибок и будет отправлено поставщику
    
    2е слово в названии метода подразумевает ГРУППУ правил проверки
    группировка используется для пропуска других правил если одно(с более высоким приоритетом)
    не прошло проверку - остальные из этой группы правил будут пропущены
    """



    # ===================================================== ПРОВЕРКИ ШТРИХКОДА
    def _rule01_barcode_empty(self, record)->bool:
        """По позиции отсутствует ШК"""
        barcode = self._get__barcode(record)
        return self.is_empty(barcode)

    def _rule02_barcode_valid_length(self, record)->bool:
        """ШК не равен 8,12 или 13 символам"""
        barcode = self._get__barcode(record)
        barcode_len = len(barcode)
        return barcode_len not in valid_barcode_lengths
        # return False if len(barcode)==8 or len(barcode)==13 else True

    # def _rule03_barcode_has_letters(self, record)->bool:
        # """В поле ШК присутствуют буквенные символы"""
        # barcode = self._get__barcode(record)
        # for l in barcode:
            # if l not in self.numbers: return True
        # return False

    def _rule04_barcode_has_letters_only(self, record)->bool:
        """В поле ШК присутствуют только буквенные символы"""
        barcode = self._get__barcode(record)
        # return not any([n in barcode for n in self.numbers])
        # return not all([n in self.numbers for n in barcode])
        return all([n not in self.numbers for n in barcode])
        # если все хорошо то должно возвращать False, в случае ошибки -- True

    def _rule05_barcode_has_zeros_only(self, record)->bool:
        """В поле ШК присутствуют только нули"""
        barcode = self._get__barcode(record)
        return all(['0' in barcode_symbol for barcode_symbol in barcode])
        # если все хорошо то должно возвращать False, в случае ошибки -- True


    # ===================================================== ПРОВЕРКИ СРОКА ГОДНОСТИ
    def _rule20_shelflife_empty(self, record)->bool:
        """Неверный тип данных, ожидается DATE"""
        shelflife = self._get__shelflife(record)
        return not isinstance(shelflife, date)

    def _rule21_shelflife_empty(self, record)->bool:
        """Отсутстует срок годности по позиции"""
        shelflife = self._get__shelflife(record)
        return self.is_empty(shelflife)

    def _rule22_shelflife_is_valid(self, record)->bool:
        """Срок годности по позиции менее сегодняшней даты"""
        shelflife = self._get__shelflife(record)
        return True if shelflife <= self.now else False


    # ===================================================== ГЕТТЕРЫ
    # для полей одной записи прайса
    #
    #значение полей прайс-листа можно подсмотреть в 
    # SELECT * FROM [SandBox].[dbo].[Price_Lists_Mapping_Fields]
    @staticmethod
    def _get__barcode(record): # получить ШтрихКод позиции из прайс-листа
        return str(record.Producer_Barcode)

    @staticmethod
    def _get__shelflife(record): # получить срок годности позиции из прайс-листа
        return record.Shelflife

    @staticmethod
    def _get__codepst(record): # получить код поставщика
        return record.Supplier_Item_Code

    @staticmethod
    def _get__title(record): # получить название позиции из прайс-листа
        return record.Supplier_Item_Name


    @staticmethod
    def is_empty(value:str)->bool:
        """Проверка пуст ли переданный value
        """
        return True if value == '' or value is None else False



class CheckerPrice(CheckerPriceRules):
    """
        Класс для проверки строк прайс-листа
        Класс будет содержать набор микроправил все из которых должны верно пройти проверку
    """

    numbers = list('0123456789')
    now = date.today()


    def __init__(self, msg=None, log=log):
        """
            supplier_price -- список моделей БД для импорта прайса
        """
        self.log = log
        if msg:
            self.msg = msg

            msg_info = minfo(msg)

            self.subject = msg_info.subject
            self.sender = msg_info.sender
            self.received = msg_info.received
            self.att_fn = msg_info.att

            self.info(f"Прайс-лист:'{self.sender}'({self.received}), тема:'{self.subject}':[{self.att_fn}]")

        self.errors = []        # список ошибок обнаруженных в ходе проверок
        self.errors_count = 0   # количество обнаруженных ошибок

        self.supplier_price_count = 0   # количество строк в прайсе до обработки

        self.rules = self.get_rules()
        self.log.info(f"Загружено {len(self.rules)} правил")

    def __call__(self, supplier_price):
        """
            Вызов экземпляра класса 
                проводит проверку всех правил,
                формирует список ошибок, 
                формирует текст емайл уведомления для менеджера поставщика
                возвращает сводную информацию для записи в лог в скрипт-инициатор
        """
        # строка протокол проверки прайса, вернем на верх для записи в логгер скрипта-инициатора
        self.supplier_price_count = len(supplier_price)

        self.proceed_rules(supplier_price)
        if self.errors_count<1: return supplier_price # если ошибок нет, выходим из себя

        supplier_price=self.remove_errorlines(supplier_price)
        self.new_supplier_price_count=len(supplier_price)

        self.errors_email_body = self.compose_email_body()
        self.log_str = self.compose_log_str()

        self.log.info(self.log_str)

        return supplier_price


    def count_occurences(self):
        """ Посчитаем сколько каких ошибок обнаружено

            https://stackoverflow.com/questions/16013485/counting-the-amount-of-occurrences-in-a-list-of-tuples

            >>>Counter({'ШК не равен 8 или 13 символам': 939, 'По позиции отсутствует ШК': 172})
        """
        # lineno, error_text, codepst, title, barcode = self.errors[0]
        if not hasattr(self, 'errors') or not self.errors: return {}
        return Counter([e[1] for e in self.errors])

    def compose_email_body(self):
        """Сформировать текст емайл сообщения для отправки менеджеру поставщика
        """
        if not self.errors: return

        errors_occurencies = """
<table border="1" style="font-size:0.7em"><caption>Статистика ошибок:</caption>
<tr><th>Тип ошибки</th><th>Количество ошибок</th></tr>"""
        self.errors_occurences = self.count_occurences()

        for k,v in self.errors_occurences.items():
            errors_occurencies += f"<tr><td>{k}</td><td>{v}</td></tr>"
        errors_occurencies += f"<tr><td><b>Всего строк:</b></td><td>{self.supplier_price_count}</td></tr>\n"
        errors_occurencies += f"<tr><td><b>Из них ошибок:</b></td><td><b>{self.errors_count}</b></td></tr></table>"

            
        errors_table_lines = ""
        for error_no, error_tuple in enumerate(self.errors, 1):
            lineno, error_text, codepst, title, barcode, shelflife = error_tuple
             # <td>{lineno+1}</td> == корректировка для соответствия номеру строки в файле
            errors_table_lines += f"""<tr>
            <td>{error_no}</td>
            <td>{lineno+1}</td>
            <td>{error_text}</td>
            <td>{codepst}</td>
            <td>{barcode}</td>
            <td>{shelflife}</td>
            <td>{title}</td>
        </tr>"""
            self.log.error(f"Ошибка №{error_no}, в строке №{lineno}, ошибка:'{error_text}', номенклатура:'{title}', код:'{codepst}', ШК:'{barcode}', СГ:'{shelflife}'")

        errors_table = f"""<table border="1" style="font-size:0.7em"><caption>Ошибки по следующим позициям:</caption>
    <tr>
        <th>№</th>
        <th>Строка<br>прайса</th>
        <th>Краткое описание ошибки</th>
        <th>Код поставщика</th>
        <th>Штрихкод</th>
        <th>Срок годности</th>
        <th>Название номенклатуры</th>
    </tr>
    {errors_table_lines}
</table>"""

        body = f"""<p style="font-size:1.1em">Уважаемые партнеры!</p><br>

От вас получен прайс-лист для Магнит Аптек
<p>тема письма: <b>{{price_msg_subject}}</b>
<br>получен: <i>{{price_msg_received}}</i>
<br>вложения: <u>{{price_msg_attachments}}</u>
</p>
В прайс-листе обнаружены ошибки, которые необходимо исправить
<br>после чего повторно отправить прайс-лист 
<hr>
<u>ВАЖНО!</u><br>
<i><b>Указанные позиции не будут участвовать в ближайших торгах</i>
<hr>
<br>
{errors_occurencies}
<br>
<p style="font-size:0.7em">
Письмо сгенерировано автоматически роботом уведомлений Магнит Аптек
<br>При возникновении вопросов необходимо писать закрепленному менеджеру
</p>
{errors_table}
<br>
"""
        return body


    def compose_log_str(self):
        """Создать строку статистики по обнаруженным ошибкам в прайс-листе
        """
        if not hasattr(self, 'errors_occurences') or not self.errors_occurences:
            self.errors_occurences = self.count_occurences()

        errors_stat_str = ""
        for k,v in self.errors_occurences.items():
            errors_stat_str += f"{k}: {v}, "

        return f"Начально/Ошибок/Итого: {self.supplier_price_count}/{self.errors_count}/{self.new_supplier_price_count}; Статистика: {errors_stat_str}"

    def proceed_rules(self, supplier_price):
        """
            Проводим проверку прайс-листа
        """
        for lineno, record in enumerate(supplier_price, 0):

            for error_tuple in self.check_rules(record):

                error_tuple = list(error_tuple)
                error_tuple.insert(0, lineno)
                error_tuple = tuple(error_tuple)
                self.errors.append(error_tuple)

        self.errors_count = len(self.errors)

        if self.errors:
            self.log.info(f"Набор из {len(supplier_price)} записей, обнаружено {len(self.errors)} ошибок")

    def remove_errorlines(self, supplier_price):
        """
            Формируем новый список строк без ошибок на основе первоначального набора
        """
        if not self.errors: return supplier_price
        self.log.debug(f"Удаляем строки с ошибками из первоначального набора, записей до:{len(supplier_price)}")

        error_lines_numbers = set([error_tuple[0] for error_tuple in self.errors])

        res=[]
        for lineno, record in enumerate(supplier_price, 0):
            if lineno in error_lines_numbers: continue
            res.append(record)


        self.log.debug(f"Завершено удаление строк с ошибками, записей после: {len(res)}")
        return res

    def check_rules(self, record):
        """
            Проверяем все имеющиеся правила на одной строке прайса(record)

            Так же используется проверка правила в рамках группы правил
                если зафиксирована ошибка в правиле(из одной группы) 
                то остальные правила группы пропускаются
        """
        skip_group=""

        for rule_name in self.rules:
            #def _rule01_barcode_empty(self, record)->bool:
            rule_group = rule_name.split('_')[2] # ['', 'rule01', 'barcode', 'empty']

            if rule_group==skip_group: 
                continue
            else:
                skip_group=""

            func = getattr(self, rule_name)
            # вызов правила привел к ошибке
            # если правило вернуло True это ошибка == проверка правила не прошла успешно!
            if func(record):
                error_text = func.__doc__.strip()
                codepst = self._get__codepst(record)
                title = self._get__title(record)
                barcode = self._get__barcode(record)
                shelflife = self._get__shelflife(record)

                yield error_text, codepst, title, barcode, shelflife

                skip_group=rule_group


    @classmethod
    def get_rules(cls)->list:
        """получаем сортированный список правил проверки - список методов класса которые начинаются с _rule##_
        """
        m = [func for func in dir(cls) if callable(getattr(cls, func)) and func.startswith("_rule")]
        return sorted(m)

    def set_values_from_record(self, record):
        """
            Найти все методы начинающиеся с _get__
            выполнить их над записью из прайс-листа и записать результат в self
            self.barcode = self._get__barcode(record)
        """
        for func in dir(self):
            if not func.startswith("_get__"): continue
            obj = getattr(self, func)
            if callable(obj):
                objname = func.split('__')[1]
                value = obj(record)
                setattr(self, objname, value)



if __name__ == "__main__":
    print("Нельзя использовать как главный запускаемый скрипт")
