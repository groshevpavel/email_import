from collections import OrderedDict
from dbfread import DBF, FieldParser

from os.path import exists


def read_dbf_file(fullfn, fields_to_read=None, encoding='auto', \
    as_dict=False, loadintomemory=True, convert_nulls=False):
    """
        Генератор, возвр 
            флаг ошибки - число (0-all good, 1 - warn, 2-error, 3-cricital)
            кортеж или словарь прочитанных данных из dbf

            fullfn - полный путь к dbf-файлу
            fields_to_read = список названий столбцов которые нужно прочитать из dbf-файла, если не задан вовзр всё
                            так же может быть списком кортежей, каждый кортеж это (название столбца, ожидаемый питон-тип, макс.размер данных для столбца)
            encoding - если не auto то попытка загрузить с указанной, иначе попытка подбора кодировки из массива encodings
    """

    # коды ошибок как в модуле logging
    _SUCCESS    = 0
    # _WARN       = 30
    # _ERROR      = 40
    # _CRITICAL   = 50
    from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL
    # коды ошибок как в модуле logging


    encodings=['cp866', 'windows-1251'] # массив перебора кодировок dbf-файла

    assert fullfn, "Не указано имя dbf-файла для загрузки"
    assert exists(fullfn), f"Указанный файл не существует '{fullfn}'"
    

    if fields_to_read:
        assert isinstance(fields_to_read, (set,tuple,list,)), "Список полей для выгрузки должен быть в виде списка, множества или кортежа"

    if encoding != "auto":
        if isinstance(encoding, (set,list,tuple,)):
            encodings = encoding
        else:
            try:
                dbf = DBF(fullfn, load=loadintomemory, encoding=encoding)
            except Exception as e:
                yield CRITICAL, f"Ошибка модуля чтения dbf, при открытии файла '{fullfn}' - {str(e)} encoding:'{encoding}'"
                return

    else:
        for enc in encodings: # перебираем массив кодировок
            try:
                dbf = DBF(fullfn, load=loadintomemory, encoding=enc)
            except UnicodeDecodeError: #если кодировка неверная, переходим к следующей
                continue
            except Exception as e: #если произошла ошибка не кодировки, пытаемся загрузить файл через парсер-класс
                try:
                    dbf = DBF(fullfn, load=loadintomemory, encoding=enc, parserclass=MyFieldParser)
                except Exception as e: #если и через парсер-класс не получилось - йелдим критикал
                    yield CRITICAL, f"Ошибка модуля чтения dbf, при открытии файла '{fullfn}' - {str(e)} autoencoding:'{enc}'"
                    return
            else:
                break

    if loadintomemory:
        recs = dbf.records[:]
    else:
        recs = dbf

    if len(recs) < 1:
        return ERROR, f"Ошибка загрузки dbf-файла - файл '{fullfn}' не содержит данных"

    #формируем словарь для полей dbf - ключ == название столбца, 
    # значение == кортеж(тип столбца(строка'D','N' и тп, не питон тип!), длина столбца)
    _fields = {f.name:(f.type,f.length,) for f in dbf.fields}


    for lineno, r in enumerate(recs, 1):
        # print(__file__, lineno, len(r))

        # проверка на случай если все значения строки пустые - 
        # так бывает если файл формируется вручную операторами микропоставщиков, а они косячат
        is_null_record = sum([bool(v) for v in r.values()])
        if is_null_record==0:
            # continue
            yield ERROR, f"Пропущено! Строка {lineno}: содержит все пустые данные"
            continue


        if convert_nulls:
            for k,v in r.items():
                if v is None:
                    if _fields[k][0] == 'D': r[k] = '0001-01-01'
                    if _fields[k][0] == 'C': r[k] = ''
                    if _fields[k][0] == 'N': r[k] = 0


        _d = OrderedDict()

        if not fields_to_read: # формируем кортеж с данными по порядку из fields_to_read
            _d = r
        else:
                # _t = tuple([r[field] for field in fields_to_read if field])
            # _d = {k:v for k,v in r.items() if k in fields_to_read}
            # _d = {f:r[f] for f in fields_to_read}
            for f in fields_to_read:
                # если в карте импорта только строка название импортируемого столбца
                if not isinstance(f, (tuple, set, list,)):
                    try:
                        _d[f] = r[f]
                    except KeyError as e:
                        return ERROR, f"Нет столбца {f}, в файле '{fullfn}', исключение: {repr(e)}"

                # здесь в fields_to_read массив кортежей - комплексное описание ожидаемых даннных 
                # (имя столбца, ожидаемый питон-тип, размер поля БД)
                else:
                    field_name, field_python_type, field_size = f

                    try:
                        data_from_file = r[field_name]
                    except KeyError as e:
                        yield ERROR, f"Нет столбца {field_name}, в файле '{fullfn}', исключение: {repr(e)}"
                        continue

                    #нормализация данных для БД площадки - подтираем косяки за поставщиками
                    #после внедрения проверки накладных и отправки брани на плохие накладные - нужно удалить
                    #
                    #если значение пусто или равно 0
                    if data_from_file is None or data_from_file == 0:
                        if 'datetime.date' in str(field_python_type):
                            data_from_file = field_python_type(2000,1,1)
                        else:
                            data_from_file = field_python_type(0)
                    #если значение пусто или равно 0

                    #если булево в виде цифры, то конвертим в булево и возвращаем
                    # if field_python_type == bool:
                        # data_from_file = field_python_type(1)


                    # print(data_from_file, type(data_from_file), field_python_type)
                    # проверка соответствия типов - данные из файла и тип столбца в БД
                    if type(data_from_file) != field_python_type:
                        try:
                            # олени шлют булево как строку а не инт
                            if field_python_type == bool and type(data_from_file)==str: 
                                data_from_file = int(data_from_file.strip())
                                
                            # у оленей, инт переменная в файле передана как флоат - пытаюсь привести к нужному типу
                            # если ош, ругаюсь
                            data_from_file = field_python_type(data_from_file)
                        except TypeError:
                            yield ERROR, f"Строка {lineno}: несоответствие типов данных столбца '{field_name}', файл -> значение: '{data_from_file}', типа:'{str(type(data_from_file)).lstrip('<class').rstrip('>')}', ожидается -> тип:'{str(field_python_type).lstrip('<class').rstrip('>')}' в файле: '{fullfn}'"
                            continue
                        except ValueError:
                            yield ERROR, f"Строка {lineno}: содержимое '{data_from_file}' столбца '{field_name}' не может быть приведено к типу:'{str(type(data_from_file)).lstrip('<class').rstrip('>')}', ожидается -> тип:'{str(field_python_type).lstrip('<class').rstrip('>')}' в файле: '{fullfn}'"
                            continue

                    # сравниваем размер данных из файла и размером из fields_to_read(суть размер столбца таблицы БД)
                    if field_size>-1 and len(str(data_from_file)) > field_size:
                        yield ERROR, f"Строка {lineno}: превышение размера вносимых данных, файл -> 'значение: {data_from_file},{type(data_from_file)}', ожидается -> '{field_name}, {str(field_python_type)}, размБД:{field_size}', в файле:{len(data_from_file)} файл:'{fullfn}'"
                        continue
                        # data_from_file = data_from_file[:field_size]



                    _d[field_name] = data_from_file


        if not as_dict:
            _t = tuple(_d.values())
            yield _SUCCESS, _t
        else:
            yield _SUCCESS, _d

    dbf.unload() # выгружаем dbf-файл из памяти



def read_entire_dbf_file(full_fn, **kwargs):
    res=[]
    for error, record in read_dbf_file(full_fn, **kwargs):
        if error:
            raise SystemError("%i, файл %s c ошибкой"%(error, full_fn))
        res.append(record)
    return res


def read_dbf_files(files, fields_to_read=None, encoding='cp866'):
    assert files, "Не передан список файлов для загрузки"

    if not isinstance(files, (set, tuple, list,)):
        assert isinstance(files, str), "Передан параметр не поддерживаемого типа: %r" % repr(files)
        files = [files]

    _files = [f for f in files if exists(f)]
    assert _files, "Переданный массив файлов содержит неверные имена файлов или полные пути к ним '%r'"%repr(files)

    assert len(files) == _files, "Некоторые файлы не существуют или неверно указан полный путь к ним (%r)" %repr(set(files)-set(_files))

    for f in _files:
        yield read_dbf_file(f, fields_to_read=fields_to_read)



class MyFieldParser(FieldParser):
    def parseN(self, field, value):
        value = value.strip(b" \x00")
        return FieldParser.parseC(self, field, value)
    
    # def parseC(self, field, value):
        # print(field.name, value)
        # if field.name=='TITLE' and b'/' in value:
            # return value.strip().replace(b'/', b'/ ').decode()
        # return FieldParser.parseC(self, field, value)

if __name__ == "__main__":
    fullfn = 'E:\\ma\\email_import\\data\\invoices\\20190321\\0116_Катрен НПК АО филиал в г.Казань (МА)\\robot@kazan.katren.ru\\invoice_289916-06_900884_7704.dbf'
    
    # fullfn = 'E:\\ma\\email_import\\data\\invoices\\20190321\\0164_ФК Гранд Капитал ТЮМЕНЬ ООО (МА)\\mfedorova@grand-capital.net\\invoice_19-0-68140_900947_6862.dbf'
    fullfn = 'E:\\ma\\email_import\\data\\invoices\\20190321\\0108_Гранд Капитал САНКТ-ПЕТЕРБУРГ\\vglukhova@grand-capital.net\\invoice_19-0-120785_900593_7914.dbf'
    
    fullfn = 'E:\\ma\\_invoice_289774-06_900557_7705-.dbf'
    
    d = DBF(fullfn, load=True, encoding='cp866', parserclass=MyFieldParser)
    print(d.loaded, d.dbversion, d.name)

    for r in d.records[:2]:
        print(r)
    # for error, record in read_dbf_file(fullfn, as_dict=True, fields_to_read=_fields, convert_nulls=False):
            # print(error, record)



