# модуль блокировок
# используется для временного блокирования роботов импорта в УАС
#
# в случае если импорт документа в УАС закончился с ошибкой (опред. типа)
# робот создает блокировку для себя, на следующий запуск
# блокировка имеет ограничение действия по времени
# при наступления времени блокирования, блокировка снимается
# для разных видов блокировок может быть разное время блокирования
# задаваться будет скорее всего в конфиге
# 
# в БД плошалки

# TABLE Python_Blocking
# id 								- id записи
# ,initiator_fullpath(str255) 	- полный путь питон-скрипта создавшего блокировку
# ,block_to_datetime				- дата время действия блокировки (если установлено, инициатор блокирует свою работу до наступления)
# ,blocked_condition				- python-сущность с описанием заблокированного действия скрипта, напр. словарь "{'orderno': 5034}"
# ,is_deleted(bit)				- флаг активности блокировки(если выставлен в True то блок игнорируется)
# ,is_deleted_change_datetime		- дата время изменения флага снятия блокировки

from utils.database import automap_tables, db_session#, pymssql_proc_call

from utils.mydt import get_now, from_now, str_datetime

from utils.incorderdict.incorderdict import IncrementalOrderedDict

tables_list = [
    'Python_Blocking',
]

# automap_tables(tables_list)
for tablename, dbtable in automap_tables(tables_list, to_main=False):
    # globals()[table] = getattr(sys.modules['__main__'], table)
    globals()[tablename] = dbtable



from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL
NO_ERRORS = 0





def get_blocking_list(initiator:str="", condition:str=""):
    """
        Получаем список событий по типу событий, если указано
    """
    # https://stackoverflow.com/questions/3325467/elixir-sqlalchemy-equivalent-to-sql-like-statement
    blocking_list = db_session.query( \
            Python_Blocking.ID \
            ,Python_Blocking.Initiator_Fullpath \
            ,Python_Blocking.Block_To_Datetime \
            ,Python_Blocking.Blocked_Condition \
        ) \
        .filter( \
            Python_Blocking.Initiator_Fullpath.ilike(f"%{initiator}%" if initiator != "" else "%"), \
            Python_Blocking.Blocked_Condition.ilike(f"%{condition}%" if condition != "" else "%"), \
            Python_Blocking.Is_Deleted == False, \
        ) \
        .all()
    
    return blocking_list

def set_blocking_done(id:int, log=None):
    """
        Устанавливаем статус Is_Deleted для записи по ID
    """
    # https://stackoverflow.com/questions/3325467/elixir-sqlalchemy-equivalent-to-sql-like-statement
    blocking = db_session.query( \
            Python_Blocking
        ) \
        .filter( \
            Python_Blocking.ID == id, \
        ) \
        .first()
    
    blocking.Is_Deleted = True
    blocking.Is_Deleted_Change_Datetime = get_now(as_str=True)
    db_session.commit()

    if log is not None:
        log.info(f"Блокировка снята для записи с ID#{id}")


def set_new_blocking_event(
        initiator_fullpath, \
        block_from_now_to_datetime, \
        blocked_condition, \
        error_text=None, \
        log=None):
    """
        Записать в БД отметку о блокирующем событии
            возвр block_from_now_to_datetime - время до которого выполняется блокировка
    """
    _d={}
    #блокриуем на указанное время от сейчас
    block_from_now_to_datetime = from_now(block_from_now_to_datetime, as_str=False)
    #секунды и миллисекунды выставляем в 0
    block_from_now_to_datetime = block_from_now_to_datetime.replace(second=0, microsecond=0)
    block_from_now_to_datetime = str(block_from_now_to_datetime)
    
    _d['Initiator_Fullpath'] = initiator_fullpath
    _d['Block_To_Datetime'] = block_from_now_to_datetime
    _d['Is_Deleted'] = False
    _d['Is_Deleted_Change_Datetime'] = get_now(as_str=True)
    _d['Blocked_Condition'] = blocked_condition

    if error_text is not None:
        _d['Error_Text'] = error_text

    list_of_models = [Python_Blocking(**_d)]

    db_session.bulk_save_objects(list_of_models, return_defaults=True)
    db_session.commit()

    if log is not None:
        log.info(f"Установлена блокировка: initiator:{initiator_fullpath}, Block_To_Datetime:{block_from_now_to_datetime}, Blocked_Condition:{blocked_condition}")

    return block_from_now_to_datetime

def get_blocked_generator(initiator, condition):
    """
        Генератор возвр словарь всех блокирующих событий
    """
    now = get_now(as_str=False) # получаем сейчас в виде datetime объекта

    _dict = IncrementalOrderedDict()

    for id, initiator_fullpath, block_to_datetime, blocked_condition in get_blocking_list(initiator, condition):
    # (4, 'E:\\ma\\email_import\\utils\\blocking.py', datetime.datetime(2019, 3, 25, 19, 8, 1), "{'orderno': 1999}")
    # (6, 'E:\\ma\\email_import\\utils\\blocking.py', datetime.datetime(2019, 3, 26, 15, 46, 8), "{'orderno': 123}")
    # (7, 'E:\\ma\\email_import\\utils\\blocking.py', datetime.datetime(2019, 3, 26, 15, 46, 8), "{'orderno': 456}")
        if block_to_datetime<=now:
            set_blocking_done(id)
            yield WARNING, f"Блок отменен! Истекло время блокирования задачи: ID#{id}, блок до: '{str_datetime(block_to_datetime)}', по условию:({blocked_condition}), инициатор:{initiator_fullpath}"
            continue

        try:
            _d = eval(blocked_condition)
        except Exception as e:
            yield ERROR, f"Пропущено! Ошибка конвертации блокирующего условия! id#:{id}, условие:'{blocked_condition}', блок до: '{str_datetime(block_to_datetime)}', по условию:({blocked_condition}), инициатор:{initiator_fullpath}"
            continue

        _dict.update(_d)

    yield NO_ERRORS, _dict


def get_blocked(initiator, condition, log=None):
    for error, res in get_blocked_generator(initiator, condition):
        if error and log is not None:
            log.log(error, res)

    # print("res",type(res), res)
    return res

if __name__ == "__main__":
    # set_new_blocking_event(__file__, '2h', repr({'orderno': 123}))
    # set_new_blocking_event(__file__, '2h', repr({'orderno': 456}))

    # l = get_blocking_list("E:\\ma\\email_import\\utils\\blocking.py")
    # l = get_blocking_list("blocking.py", "order")
    l= get_blocked(__file__, 'order')
    print(l['order'])
