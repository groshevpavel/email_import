import json

import asyncio
import aiohttp

import time

from .uas_convert import uas_order_convert, uas_accept_convert, uas_invoice_convert


# http://www.quizful.net/post/thread-synchronization-in-python

_headers = {
  # hided
}



class UAS_Request(object):
    _server_name = "https://hided"
    _user, _passw = ('login', 'password')
    _apipath = '/webservice/path/'
    _apifull = f"{_server_name}{_apipath}"

    def __init__(self, uas_ws_path=_apifull, **kwargs):
        self.api_http_path = uas_ws_path
        if not self.api_http_path.endswith('/'): self.api_http_path += '/'

        # self.session = requests.Session()
        self.session = None

        self.auth = aiohttp.BasicAuth(self._user, self._passw)
        self.timeout = aiohttp.ClientTimeout(total=30*60) # ожидаем уас до N минут

        self.order_created_guid = ""

    async def get_session(self):
        if self.session is None: 
            # self.session = aiohttp.ClientSession(headers=_headers)
            self.session = aiohttp.ClientSession(auth=self.auth, timeout=self.timeout)
            # self.session.auth = self.auth

    async def request(self, method, request_str, **kwargs):
        await self.get_session()
        assert self.session, "Не инициализированно подключение к серверу API"

        try:
            async with self.session as session:
                return await session.request(method, request_str, **kwargs)
                # return await resp if not kwargs['json'] == True else await resp.json()
                # return await resp.read()
        except Exception as e:
            print("request Exception:", e)
            # self.resp_text = await resp.text()
            raise

    def get_full_api_path(self, uas_api_method):
        self.request_str = f"{self.api_http_path}{uas_api_method}"
        return self.request_str

    async def post(self, uas_api_method, **kwargs):
        request_str = self.get_full_api_path(uas_api_method)
        return await self.request('POST', request_str, **kwargs)

    async def get(self, uas_api_method, **kwargs):
        request_str = self.get_full_api_path(uas_api_method)
        return await self.request('GET', request_str, **kwargs)

    async def getjson(self, uas_api_method, **kwargs):
        return await self.get(uas_api_method, json=True)

    async def check_error(self, resp):
        status = resp.status
        resp_text = await resp.text()

        if status == 400:
            raise ValueError(f"Веб-сервис УАС вернул ошибку - Некорректные параметры, статус {status}, ответ {resp_text}")
        if status == 500:
            raise SystemError(f"Веб-сервис УАС вернул ошибку - Ошибка сервера, статус {status}, ответ {resp_text}")

        if status not in range(200,299):
            raise SystemError(f"Неведомая ошибка веб-сервиса УАС - статус {status}, ответ {resp_text}")
        raise SystemError(f"Необрабатываемый статус от веб-сервиса УАС - статус {status}, ответ {resp_text}")



    async def uas_post_method(self, method, data):
        assert method, "Не передан необходимый параметр -- метод для публикации в УАС"
        assert data, "Не передан необходимый параметр -- данные для публикации в УАС методом '{method}'"

        if not isinstance(data, str): # пришел не json
            data = json.dumps(data, indent=4, ensure_ascii = False)

        resp = await self.post(method, data=data.encode('utf-8'))
        # http://docs.python-requests.org/en/master/user/quickstart/
        # resp.raise_for_status()
        # http://docs.python-requests.org/en/master/user/quickstart/
        
        status = resp.status

        #в случае попытки внесения уже созданного документа
        # веб-сервис возвращает его гуид, но не через 200 статус
        # поэтому имеют место такие косты
        if 'id' in resp.headers and status != 200:
            self.order_created_guid = resp.headers['id']
            return self.order_created_guid

        if status != 200:
            await self.check_error(resp)

        if not 'id' in resp.headers:
            raise SystemError(f"Выполнено успешно, но веб-сервис не вернул параметр с GUID документа, {repr(resp.headers)}")

        self.order_created_guid = resp.headers['id']
        return self.order_created_guid


    async def uas_post_order(self, orderdata, order_guid=None):
        return await self.uas_post_method('order', orderdata)

    async def uas_post_accept(self, acceptdata, order_guid=None):
        assert isinstance(acceptdata, (dict, str,)), f"Для внесения акцепта - передан неверный тип данных >{type(acceptdata)}"

        if order_guid:
            acceptdata['Id'] = order_guid.strip()\
                if isinstance(order_guid, str)\
                    else str(order_guid).strip()

        # if acceptdata['Status'] != 'Подтвержден' or 'Status' not in acceptdata:
            # acceptdata['Status'] = 'Подтвержден'

        assert 'Id' in acceptdata, "Для внесения акцепта, не передан GUID документа УАС"
        assert 'Status' in acceptdata, "Для внесения акцепта, не передан статус документа УАС"

        if not isinstance(acceptdata, str):
            assert acceptdata['Status'] != 'Подтвержден',\
                "Для внесения акцепта, установлен неверный статус документа УАС"

        #веб-сервис запилен таким образом, что и "заказ" и "акцепт" суются через один веб-сервис-метод "order" :/
        return await self.uas_post_method('order', acceptdata)


    async def uas_post_invoice(self, invoicedata):
        # if not isinstance(invoicedata, str): # пришел не json
            # assert isinstance(invoicedata, (dict, list, set, tuple,)), "Передана неверная структура для пе"
            # invoicedata = json.dumps(invoicedata, indent=4, ensure_ascii = False)

        assert isinstance(invoicedata, str), f"Для внесения акцепта - передан неверный тип данных >{type(invoicedata)}, ожидается json в виде строки"

        return await self.uas_post_method('bill', invoicedata)


    async def asynchronous(self, methods):
        start = time.time()

        futures = [self.getjson(m) for m in methods]

        for i, future in enumerate(asyncio.as_completed(futures)):
            result = await future
            # print('{} {}'.format(">>" * (i + 1), result ))
            print('{} {}'.format(">>" * (i + 1), len(result) ))

        print('took: {:.2f} seconds'.format(time.time() - start))
        # await self.session.close()

    def start(self, *args, **kwargs):
        ioloop = asyncio.get_event_loop()
        ioloop.run_until_complete(self.asynchronous(*args, **kwargs))
        ioloop.run_until_complete(asyncio.sleep(0.250))
        ioloop.close()




async def post_accept(acceptdata, uas_order_guid=None):
    assert acceptdata is not None

    accept_data_for_uas = uas_accept_convert(acceptdata)

    r = UAS_Request()
    uas_doc_guid = await r.uas_post_accept(accept_data_for_uas, order_guid=uas_order_guid)

    # print("Создан документ, ", uas_doc_guid)
    return uas_doc_guid


if __name__ == "__main__":
    methods = ['divisions', 'regions', 'products', 'manufacturers', 'barcodes', 'suppliers', 'agreements']
    u = UAS_Request()
    u.start(methods)
