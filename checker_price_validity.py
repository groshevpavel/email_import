import sys
sys.path.insert(0, 'e:\\ma\\email_import')

# KОМАНДА РАЗВИТИЯ ИНФОРМАЦИОННЫХ СИСТЕМ АПТЕК

# [ТЕМА]
# Напомнинание КА о необходимости обновить прайс лист

# [ССЫЛКА]
# https://localjira/browse/TESTAPTEK-1894

# [ОПИСАНИЕ]
# При истечении срока актуальности прайс листа отправлять КА 
# напоминание о необходиомости отправить актуальный прайс лист

# [РЕАЛИЗАЦИЯ]
# Взять даты поступления прайсов - список шапок прайсов, табл Price_Lists(столбец Date)
# Взять количество просрочки прайса (в минутах) из Suppliers(Validity_Price_List, Email_Manager)
# Вычислить дельту(в минутах) между получением прайса и сейчас
# Если превышает 
    # - если уведомление еще не отправлялось:
        # - отправить уведомление на емайл менеджера
        # - зафиксировать факт отправки уведомления


# from utils.exchange_wrapper import apteki_orders_account as apteki_mail_account
from utils.exchange_wrapper import apteki_ext_account as apteki_mail_account

from config import report_to_price_list_expired, \
    new_log_levels_price_list_expired, price_list_expired_body

from utils.log import log
log = log("check_price_validity", "DEBUG", take_path=__file__, add_folder=''
    ,mail_account = apteki_mail_account
    #для отправки на внешние адреса нужно выставить в False
    ,send_only=False
    # ,exchange_no=True
    ,http_no=True, stream_no=True
    # ,mail_to=error_report_addr['to'], mail_cc=error_report_addr['cc']
    ,new_levels = new_log_levels_price_list_expired
    ,report_to = report_to_price_list_expired
    ,logfilesize=1 # размер лог-файлов
    ,backupCount=2 # количество лог-файлов
    # ,fmt='%(asctime)s %(message)s'
    ,fmt='%(asctime)s %(levelname)-10s %(funcName)-20s %(message)s'
    ,mail_logging_level = 25 #чуть выше logging.INFO см. в config
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




from utils.mydt import get_now, from_now


from utils.dotdict import dotdict


from utils.database import reflect_all_tables_to_declarative
tables=[
    'Price_Lists'
    ,'Suppliers'
    # ,'integration.Suppliers'
]
mssql_session = reflect_all_tables_to_declarative(tables)


from sqlite.models.price_lists_validity_annotation import Price_Lists_Validity_Annotation
from sqlite import sqlite_session

def save_annotation_event(supplier_id:int):
    plva = Price_Lists_Validity_Annotation(supplier_id=supplier_id)
    try:
        sqlite_session.add(plva)
        sqlite_session.commit()
    except Exception as e:
        log.critical(f"Ошибка записи в БД SQLITE: {e}")
        sqlite_session.rollback()

def load_annotation_events(supplier_id:int):
    date_now=get_now(as_str=False).date()
    return sqlite_session.query(
        Price_Lists_Validity_Annotation.sended)\
        .filter(Price_Lists_Validity_Annotation.supplier_id==supplier_id)\
        .filter(Price_Lists_Validity_Annotation.sended>=date_now)\
        .all()


def has_annotation_events(supplier_id:int)->bool:
    """Проверяем: отправляли сегодня уведомление поставщику"""
    return bool(load_annotation_events(supplier_id))


def get_suppliers_dict()->dict:
    # собираем словарь словарей, ключ - id поставщика, 
    # значение - словарь код поставщика, название, емайл менеджера, время жизни прайса
    r = mssql_session.query(Suppliers) \
        .filter(Suppliers.Is_Deleted==0) \
        .values(Suppliers.ID
            ,Suppliers.Code
            ,Suppliers.Title
            ,Suppliers.Email_Manager
            ,Suppliers.Validity_Price_List)

    res={}
    for _t in r:
        res[_t[0]] = dict(zip(['code', 'title', 'email_manager', 'validity'], _t[1:]))
    return res

def yield_price_lists_recieve_dates():
    """Генератор возвр словарь запись таблицы Price_Lists"""
    res = mssql_session.query(Price_Lists) \
        .filter(Price_Lists.Is_Deleted==0) \
        .order_by(Price_Lists.Date.asc()) \
        .values(*Price_Lists.__table__.c)

    for r in res:
        yield dotdict(zip(
            Price_Lists.__table__.c.keys()
            ,r
        ))





if __name__ == "__main__":
    suppliers = get_suppliers_dict()

    for pl in yield_price_lists_recieve_dates():
    # {'ID': 24382, 'Supplier_ID': 110, 'Is_Deleted': False, 'Date': datetime.datetime(2019, 2, 25, 13, 18, 54), 'Title': '110__Салон Спорт-Сервис__20190225-131854.dbf', 'Date_Deleted': None, 'Last_Modified': 1551090252}
        # print(f"{pl.Supplier_ID}\t {pl.Date} \t{pl.Title}")
        supplier_id = pl.Supplier_ID

        supplier = dotdict(suppliers[supplier_id])

        validity_in_seconds = supplier.validity
        email_manager = supplier.email_manager

        price_validity_in_seconds = get_now(as_str=False) - pl.Date
        price_validity_in_seconds = price_validity_in_seconds.total_seconds()
        price_validity_in_seconds = int(price_validity_in_seconds)

        if price_validity_in_seconds > validity_in_seconds and not has_annotation_events(supplier_id):
            # print(price_validity_in_seconds, validity_in_seconds, pl.Date, pl.Supplier_ID, pl.Title, sep='\t')

            log.debug(f"Пост.ID#:{pl.Supplier_ID}, КодТандер:'{supplier.code}', емайл отв. менеджера: '{email_manager}'")
            log.debug(f"Прайс(получен/заголовок): {pl.Date} / {pl.Title}")
            log.debug(f"Прошло секунд с момента получения прайса: {price_validity_in_seconds}"
                f", уст. актуальность прайса(сек): {validity_in_seconds}"
            )

            subject = f"Ваш прайс-лист не участвует в торгах на Магнит Аптеки"
            body = price_list_expired_body.format(**{'plDate':pl.Date})

            log.price_expired(f"Отправка на {email_manager}"
                ,extra={
                    'subject'   :subject,
                    'body'      :body,
                    # 'ps'        :f"Тестовая версия! В боевом режиме письмо будет отправлено на <b>{email_manager}</b>",

                    'to':email_manager,
                    # 'to':'groshev_pp@magnit.ru',
                }
            )

            #сохраняем флаг отправки уведомления в sqlite
            save_annotation_event(supplier_id)

            # break

